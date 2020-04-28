package main

import (
	"flag"
	"github.com/ventuary-lab/node-payout-manager/logger"

	"github.com/ventuary-lab/node-payout-manager/blockchain/transactions"
	"strconv"
	"time"


	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ventuary-lab/node-payout-manager/client"
	"github.com/ventuary-lab/node-payout-manager/config"
	"github.com/ventuary-lab/node-payout-manager/rpd"
	"github.com/ventuary-lab/node-payout-manager/storage"
)

const (
	defaultConfigFileName = "config.json"
)

var currLogger *logger.Logger

func main() {
	var confFileName string
	flag.StringVar(&confFileName, "config", defaultConfigFileName, "set config path")
	flag.Parse()

	currLogger = logger.NewLogger("./logs")
	cfg, err := config.Load(confFileName)
	if err != nil {
		currLogger.Error(err.Error())
	}

	var nodeClient = client.New(cfg.NodeURL, cfg.ApiKey)
	for {
		err := Scan(nodeClient, cfg)
		if err != nil {
			currLogger.Error(err.Error())
		}
		time.Sleep(time.Duration(cfg.SleepSec) * time.Second)
	}
}

func Scan(nodeClient client.Node, cfg config.Config) error {
	rpdConfig := rpd.Config{
		Sender:           cfg.Sender,
		NeutrinoContract: cfg.NeutrinoContract,
		AssetId:          cfg.AssetId,
		RpdContract:      cfg.RPDContract,
	}
	currLogger.Info("Start scan")

	// convert all balance waves -> usd-n

	swapHash, err := rpd.SwapAllBalance(nodeClient, rpdConfig)

	if err != nil {
		return err
	}
	if swapHash != "" {
		errChan := nodeClient.WaitTx(swapHash)
		if err := <-errChan; err != nil {
			return err
		}
		currLogger.Info("Swap tx: " + swapHash)
	}

	db, err := leveldb.OpenFile(storage.DbPath, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	lastPaymentHeight, err := storage.LastPaymentHeight(db)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	} else if lastPaymentHeight == 0 {
		lastPaymentHeight = cfg.DefaultLastPaymentHeight
	}
	currLogger.Info("Last payment height: " + strconv.Itoa(lastPaymentHeight))

	lastHeight, err := storage.LastScanHeight(db)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	} else if lastHeight == 0 {
		lastHeight = lastPaymentHeight
	}
	currLogger.Info("Last scan height: " + strconv.Itoa(lastHeight))

	height, err := nodeClient.GetHeight()
	if err != nil {
		return err
	}

	currLogger.Info("Height: " + strconv.Itoa(height))

	currLogger.Info("Get contract state")
	contractState, err := nodeClient.GetStateByAddress(cfg.RPDContract)
	if err != nil {
		return err
	}
	balances := rpd.StateToBalanceMap(contractState, rpdConfig)
	if len(balances) == 0 {
		currLogger.Info("Neutrino stakers not found")
		return nil
	}
	currLogger.DebugJson("Contract state: ", balances)

	currLogger.Info("Recovery balance")
	balancesByHeight, err := rpd.RecoveryBalance(nodeClient, rpdConfig, balances, height, lastHeight)
	if err != nil {
		return err
	}
	//currLogger.Debug("Balance: ", balancesByHeight)

	currLogger.Info("Write to level db")
	for height, balances := range balancesByHeight {
		err := storage.PutBalances(db, height, balances)
		if err != nil {
			return err
		}
	}
	err = storage.PutScanHeight(db, height)
	if err != nil {
		return err
	}

	neutrinoContractState, err := nodeClient.GetStateByAddress(cfg.NeutrinoContract)

	if err != nil {
		return err
	}
	if height >= lastPaymentHeight+cfg.PayoutInterval && neutrinoContractState["balance_lock_waves_"+cfg.Sender].Value.(float64) == 0 {
		currLogger.Info("Start payout rewords")
		balance, err := nodeClient.GetBalance(cfg.Sender, cfg.AssetId)
		if balance == 0 {
			currLogger.Info("Await pacemaker oracle or swap")
			return nil
		}
		if err != nil {
			return err
		}
		currLogger.Info("Total balance: " + strconv.FormatFloat(balance, 'f', 0, 64))
		currLogger.Info("Calculate rewords")

		rawRewards, err := rpd.CalculateRewords(db, balance, height, lastPaymentHeight)

		sc := client.StakingCalculator{Url: &cfg.StakingCalculatorUrl}
		scp := rpd.BalanceMapToStakingPaymentList(rawRewards)

		calcResult := sc.FetchStakingRewards(scp)
		var rewardTxs []transactions.Transaction

		rewardTxs = append(rewardTxs, rpd.CreateDirectMassRewardTransactions(calcResult.Direct, rpdConfig)...)
		rewardTxs = append(rewardTxs, rpd.CreateReferralMassRewardTransactions(calcResult.Ref, rpdConfig)...)

		currLogger.Info("Sign and broadcast")
		for _, rewordTx := range rewardTxs {
			if err := nodeClient.SignTx(&rewordTx); err != nil {
				return err
			}
			currLogger.Info("Reword tx hash: " + rewordTx.ID)
			currLogger.DebugJson("Reword tx: ", rewordTx)

			if err := nodeClient.Broadcast(rewordTx); err != nil {
				return err
			}
		}
		//TODO
		if err := storage.PutPaymentHeight(db, height); err != nil {
			return err
		}
	}
	currLogger.Info("End scan")
	return nil
}
