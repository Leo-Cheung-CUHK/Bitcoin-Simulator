#include "ns3/application.h"
#include "ns3/event-id.h"
#include "ns3/ptr.h"
#include "ns3/traced-callback.h"
#include "ns3/address.h"
#include "ns3/log.h"
#include <fstream>
#include <time.h>
#include <sys/time.h>
#include <array>
#include "transaction.h"
// for mac
#include "cryptopp/cryptlib.h"
#include "cryptopp/sha.h"
#include "cryptopp/hex.h"
// #include "cryptopp/byte"
// for linux
// #include "crypto++/cryptlib.h"
// #include "crypto++/sha.h"
// #include "crypto++/hex.h"

NS_LOG_COMPONENT_DEFINE("DashTransaction");

namespace ns3 {

/**
 *
 * Class Transaction functions
 *
 */

Transaction::Transaction(double transactionSizeBytes, std::string transactionHash, std::string transactionShortHash) : m_transactionSizeBytes(transactionSizeBytes), m_transactionHash(transactionHash), m_transactionShortHash(transactionShortHash) {}

Transaction::Transaction() = default;

Transaction::Transaction (const Transaction &transactionSource)
{
  m_transactionSizeBytes = transactionSource.m_transactionSizeBytes;
  // m_mempoolTransactionHeight = transactionSource.m_mempoolTransactionHeight;
  m_transactionHash = transactionSource.m_transactionHash;
	m_transactionShortHash = transactionSource.m_transactionShortHash;
  // m_nodeId = transactionSource.m_nodeId;
	// m_timeCreated = transactionSource.m_timeCreated;
	// m_timeReceived = transactionSource.m_timeReceived;
	// m_createdAtIpv4 = transactionSource.m_createdAtIpv4;

}

Transaction::~Transaction (void)
{
}

double
Transaction::GetTransactionSizeBytes (void) const
{
  return m_transactionSizeBytes;
}

void
Transaction::SetTransactionSizeBytes (double transactionSizeBytes)
{
  m_transactionSizeBytes = transactionSizeBytes;
}

std::string
Transaction::GetTransactionHash (void) const
{
	return m_transactionHash;
}

void
Transaction::SetTransactionHash (std::string transactionHash)
{
	m_transactionHash = transactionHash;
}

std::string
Transaction::GetTransactionShortHash (void) const
{
	return m_transactionShortHash;
}

void
Transaction::SetTransactionShortHash (std::string transactionShortHash)
{
	m_transactionShortHash = transactionShortHash;
}

std::string
sha256(const std::string data)
{
    byte const* pbData = (byte*) data.data();
    unsigned int nDataLen = data.size();
    byte abDigest[CryptoPP::SHA256::DIGESTSIZE];
    CryptoPP::SHA256().CalculateDigest(abDigest, pbData, nDataLen);

    return std::string((char*)abDigest);
}


Mempool::Mempool(void)
{
    m_totalTransactions = 0;
    Transaction randomTransaction(0,"defaultHash","shortHash");
    AddTransaction(randomTransaction);
}

Mempool::~Mempool(void)
{
}
// Mempool -Lihao
void
Mempool::AddTransaction(const Transaction& newTransaction)
{
	std::pair<std::string,Transaction> transactionPair (newTransaction.GetTransactionShortHash(),newTransaction);
	std::string shortHash = newTransaction.GetTransactionShortHash();
	if (m_transactions.count(shortHash))
	{
		m_transactions[shortHash] = newTransaction;
	}
	else
	{
		m_transactions.insert(transactionPair);
	}

	// std::string key = newTransaction.GetTransactionShortHash();
	// std::vector<Transaction> vecTrans;
	// vecTrans.push_back(newTransaction);
	// m_transactions[key] = vecTrans;
		// m_transactions.push_back(newTransaction);
}

int
Mempool::GetMempoolSize(void) const
{
	NS_LOG_FUNCTION(this);
	return m_transactions.size();
}

std::map<std::string,Transaction>
Mempool::GetMempoolTransactions (void) const
{
	return m_transactions;
}

bool
Mempool::HasShortTransaction (std::string shortHash)
{
	auto search = m_transactions.find (shortHash);
	if (search != m_transactions.end())
		return true;
	return false;
}

void
Mempool::DeleteTransactionWithShortHash(std::string shortHash)
{
	m_transactions.erase(shortHash);
	m_totalTransactions--;

}

Transaction
Mempool::GetTransactionWithShortHash (std::string shortHash)
{
	 auto search = m_transactions.find (shortHash);
	 if (search == m_transactions.end())
	 {
		 Transaction newTransaction (0,"defaultHash","shortHash");
		 return newTransaction;

	 }

	 return search->second;
}

} //namespace ns3

