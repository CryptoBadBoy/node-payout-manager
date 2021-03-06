package rpd

import (
	"math"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"

	"github.com/ventuary-lab/node-payout-manager/storage"

	"github.com/ventuary-lab/node-payout-manager/blockchain/transactions"

	"github.com/ventuary-lab/node-payout-manager/state"

	"github.com/ventuary-lab/node-payout-manager/assets"
	"github.com/ventuary-lab/node-payout-manager/blockchain/neutrino"
	"github.com/ventuary-lab/node-payout-manager/client"
)

type Config struct {
	Sender           string
	NeutrinoContract string
	AssetId          string
	RpdContract      string
}

func SwapAllBalance(node client.Node, rpdConfig Config) (string, error) {
	var subtrahend float64 = neutrino.InvokeFee + neutrino.MaxTransferFeeSafe
	balance, err := node.GetBalance(rpdConfig.Sender, assets.WavesAssetId)
	if err != nil {
		return "", err
	}

	if balance < neutrino.MinSwapWavesAmount+subtrahend {
		return "", nil
	}

	tx := neutrino.CreateSwapToNeutrinoTx(rpdConfig.Sender, rpdConfig.NeutrinoContract, balance-subtrahend)
	if err := node.SignTx(&tx); err != nil {
		return "", err
	}
	if err := node.Broadcast(tx); err != nil {
		return "", err
	}

	return tx.ID, nil
}

func RecoveryBalance(node client.Node, rpdConfig Config, balances storage.BalanceMap, height int, lastTxHeight int) (map[int]storage.BalanceMap, error) {
	var invokeTxs []transactions.Transaction
	lastTxHash := ""
getTxLoop:
	for {
		txs, err := node.GetTransactions(rpdConfig.RpdContract, lastTxHash)
		if err != nil {
			return nil, err
		}

		if txs == nil {
			break getTxLoop
		}
		for _, v := range txs {
			if v.Height < lastTxHeight {
				break getTxLoop
			} else {
				invokeTxs = append(invokeTxs, v)
			}
			lastTxHash = v.ID
		}
	}

	balanceByHeight := make(map[int]storage.BalanceMap)
	groupedTxs := transactions.GroupByHeightAndFunc(invokeTxs)
	for i := height; i > lastTxHeight; i-- {
		balanceByHeight[i] = make(storage.BalanceMap)
		balances.Copy(balanceByHeight[i])
		for _, v := range groupedTxs[i][neutrino.LockRPDFunc] {
			if v.InvokeScriptBody.DApp != rpdConfig.RpdContract || len(v.InvokeScriptBody.Payment) != 1 || *v.InvokeScriptBody.Payment[0].AssetId != rpdConfig.AssetId {
				continue
			}
			balances[v.Sender] -= float64(v.InvokeScriptBody.Payment[0].Amount)
		}
		for _, v := range groupedTxs[i][neutrino.UnlockRPDFunc] {
			if v.InvokeScriptBody.DApp != rpdConfig.RpdContract || v.InvokeScriptBody.Call.Args[1].Value.(string) != rpdConfig.AssetId {
				continue
			}
			balances[v.Sender] += v.InvokeScriptBody.Call.Args[0].Value.(float64)
		}
	}
	return balanceByHeight, nil
}

func CalculateRewords(db *leveldb.DB, totalProfit float64, height int, paymentHeight int) (storage.BalanceMap, error) {
	period := height - paymentHeight
	profitByBlock := totalProfit / float64(period)
	rewords := make(storage.BalanceMap)
	for i := paymentHeight + 1; i <= height; i++ {
		balances, err := storage.Balances(db, i)
		if err != nil {
			return nil, err
		}
		var totalBalance float64
		for _, v := range balances {
			totalBalance += v
		}

		for k, v := range balances {
			share := v / totalBalance
			rewords[k] += share * profitByBlock
		}
	}
	return rewords, nil
}

func BalanceMapToStakingPaymentList(bm storage.BalanceMap) []client.StakingCalculationPayment {
	res := make([]client.StakingCalculationPayment, len(bm))

	for address, value := range bm {
		res = append(res, client.StakingCalculationPayment{ Amount: int64(value), Recipient: address })
	}

	return res
}

func StateToBalanceMap(contractState map[string]state.State, rpdConfig Config) storage.BalanceMap {
	balances := make(storage.BalanceMap)
	for key, value := range contractState {
		args := strings.Split(key, "_")
		if len(args) != 4 || args[0] != "rpd" || args[1] != "balance" || args[2] != rpdConfig.AssetId {
			continue
		}
		amount, ok := value.Value.(float64)
		if ok {
			balances[args[3]] = amount
		}
	}
	return balances
}

func GatherTransfersForMassRewardTxsFromSCP(rewords *[]client.StakingCalculationPayment) []transactions.Transfer {
	transfers := make([]transactions.Transfer, 0, len(*rewords))

	for _, value := range *rewords {
		roundValue := math.Floor(float64(value.Amount))
		if roundValue > 0 {
			transfers = append(transfers, transactions.Transfer{Amount: int64(roundValue), Recipient: value.Recipient})
		}
	}

	return transfers
}

func GatherTransfersForMassRewardTxs(rewords *storage.BalanceMap) []transactions.Transfer {
	transfers := make([]transactions.Transfer, 0, len(*rewords))

	for address, value := range *rewords {
		roundValue := math.Floor(value)
		if roundValue > 0 {
			transfers = append(transfers, transactions.Transfer{Amount: int64(roundValue), Recipient: address})
		}
	}

	return transfers
}

//func CreateMassRewardTxs(rewords storage.BalanceMap, rpdConfig Config) []transactions.Transaction {
//	transfers := GatherTransfersForMassRewardTxs(&rewords)
//
//	rewardTxs := make([]transactions.Transaction, 0, int(math.Ceil(float64(len(transfers))/100)))
//	lenTransfers := len(transfers)
//	for i := 0; i < lenTransfers; i += 100 {
//		endIndex := i + 100
//
//		if endIndex > lenTransfers {
//			endIndex = lenTransfers
//		}
//
//		actualTransfers := transfers[i:endIndex]
//		rewardTx := transactions.New(transactions.MassTransfer, rpdConfig.Sender)
//		rewardTx.NewMassTransfer(actualTransfers, &rpdConfig.AssetId)
//		rewardTxs = append(rewardTxs, rewardTx)
//	}
//	return rewardTxs
//}

func CreateDirectMassRewardTransactions(rewords []client.StakingCalculationPayment, rpdConfig Config) []transactions.Transaction {
	transfers := GatherTransfersForMassRewardTxsFromSCP(&rewords)

	rewardTxs := make([]transactions.Transaction, 0, int(math.Ceil(float64(len(transfers))/100)))
	lenTransfers := len(transfers)
	for i := 0; i < lenTransfers; i += 100 {
		endIndex := i + 100

		if endIndex > lenTransfers {
			endIndex = lenTransfers
		}

		actualTransfers := transfers[i:endIndex]

		rewardTx := transactions.New(transactions.MassTransfer, rpdConfig.Sender)
		rewardTx.NewMassTransfer(actualTransfers, &rpdConfig.AssetId)
		rewardTxs = append(rewardTxs, rewardTx)
	}
	return rewardTxs
}

func CreateReferralMassRewardTransactions(rewords []client.StakingCalculationPayment, rpdConfig Config) []transactions.Transaction {
	transfers := GatherTransfersForMassRewardTxsFromSCP(&rewords)

	rewardTxs := make([]transactions.Transaction, 0, int(math.Ceil(float64(len(transfers))/100)))
	lenTransfers := len(transfers)
	for i := 0; i < lenTransfers; i += 100 {
		endIndex := i + 100

		if endIndex > lenTransfers {
			endIndex = lenTransfers
		}

		actualTransfers := transfers[i:endIndex]

		rewardTx := transactions.New(transactions.MassTransfer, rpdConfig.Sender)
		rewardTx.NewMassTransfer(actualTransfers, &rpdConfig.AssetId)
		rewardTx.Attachment = transactions.MassTransferReferralAttachment

		rewardTxs = append(rewardTxs, rewardTx)
	}
	return rewardTxs
}
