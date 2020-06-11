/**
 * This file contains the definitions of the functions declared in bitcoin-node.h
 */

#include "ns3/address.h"
#include "ns3/address-utils.h"
#include "ns3/log.h"
#include "ns3/inet-socket-address.h"
#include "ns3/inet6-socket-address.h"
#include "ns3/node.h"
#include "ns3/socket.h"
#include "ns3/udp-socket.h"
#include "ns3/simulator.h"
#include "ns3/socket-factory.h"
#include "ns3/packet.h"
#include "ns3/trace-source-accessor.h"
#include "ns3/udp-socket-factory.h"
#include "ns3/tcp-socket-factory.h"
#include "ns3/uinteger.h"
#include "ns3/double.h"
#include "bitcoin-node.h"
#include <random>
#include <cstdlib>
#include <array>
#include "cryptopp/cryptlib.h"
#include "cryptopp/sha.h"
#include "cryptopp/hex.h"
#include "../../rapidjson/document.h"
#include "../../rapidjson/writer.h"
#include "../../rapidjson/stringbuffer.h"

#include <mutex> //std::mutex, std::lock_guard, std::adopt_lock  Adding for access queue resource -Lihao

std::mutex TX_mtx; //mutex for sendQueueTimes section - Lihao
std::mutex RX_mtx; //mutex for receiveQueueTimes section - Lihao

namespace ns3
{

NS_LOG_COMPONENT_DEFINE("BitcoinNode");

NS_OBJECT_ENSURE_REGISTERED(BitcoinNode);

TypeId
BitcoinNode::GetTypeId(void)
{
  static TypeId tid = TypeId("ns3::BitcoinNode")
                          .SetParent<Application>()
                          .SetGroupName("Applications")
                          .AddConstructor<BitcoinNode>()
                          .AddAttribute("Local",
                                        "The Address on which to Bind the rx socket.",
                                        AddressValue(),
                                        MakeAddressAccessor(&BitcoinNode::m_local),
                                        MakeAddressChecker())
                          .AddAttribute("Protocol",
                                        "The type id of the protocol to use for the rx socket.",
                                        TypeIdValue(UdpSocketFactory::GetTypeId()),
                                        MakeTypeIdAccessor(&BitcoinNode::m_tid),
                                        MakeTypeIdChecker())
                          .AddAttribute("InvTimeoutMinutes",
                                        "The timeout of inv messages in minutes",
                                        TimeValue(Minutes(20)),
                                        MakeTimeAccessor(&BitcoinNode::m_invTimeoutMinutes),
                                        MakeTimeChecker())
                          .AddAttribute("FixedTransactionSize",
                                        "The fixed size of transaction",
                                        UintegerValue(0),
                                        MakeUintegerAccessor(&BitcoinNode::m_fixedTransactionSize),
                                        MakeUintegerChecker<uint32_t>())
                          .AddAttribute("FixedTransactionIntervalGeneration",
                                        "The fixed time to wait between two consecutive transactions generations",
                                        DoubleValue(0),
                                        MakeDoubleAccessor(&BitcoinNode::m_fixedTransactionTimeGeneration),
                                        MakeDoubleChecker<double>())
                          .AddTraceSource("Rx",
                                          "A packet has been received",
                                          MakeTraceSourceAccessor(&BitcoinNode::m_rxTrace),
                                          "ns3::Packet::AddressTracedCallback");
  return tid;
}

BitcoinNode::BitcoinNode(void) : m_bitcoinPort(8333), m_secondsPerMin(60), m_isMiner(false), m_countBytes(4), m_bitcoinMessageHeader(90),
                                 m_inventorySizeBytes(36), m_getHeadersSizeBytes(72), m_headersSizeBytes(81), m_blockHeadersSizeBytes(81)
{
  NS_LOG_FUNCTION(this);
  m_socket = 0;
  m_meanBlockReceiveTime = 0;
  m_previousBlockReceiveTime = 0;
  m_meanBlockPropagationTime = 0;
  m_meanBlockSize = 0;
  m_numberOfPeers = m_peersAddresses.size();

  //random seed to be generated once per instant
  // std::random_device rd;
  //m_generator.seed(rand());

  if (m_fixedTransactionTimeGeneration > 0)
    m_nextTransactionTime = m_fixedTransactionTimeGeneration;
  else
    m_nextTransactionTime = 0;

  if (m_fixedTransactionSize > 0)
    m_nextTransactionSize = m_fixedTransactionSize;
  else
    m_nextTransactionSize = 0;
}

BitcoinNode::~BitcoinNode(void)
{
  NS_LOG_FUNCTION(this);
}

Ptr<Socket>
BitcoinNode::GetListeningSocket(void) const
{
  NS_LOG_FUNCTION(this);
  return m_socket;
}

std::vector<Ipv4Address>
BitcoinNode::GetPeersAddresses(void) const
{
  NS_LOG_FUNCTION(this);
  return m_peersAddresses;
}

void BitcoinNode::SetPeersAddresses(const std::vector<Ipv4Address> &peers)
{
  NS_LOG_FUNCTION(this);
  m_peersAddresses = peers;
  m_numberOfPeers = m_peersAddresses.size();
}

void BitcoinNode::SetPeersDownloadSpeeds(const std::map<Ipv4Address, double> &peersDownloadSpeeds)
{
  NS_LOG_FUNCTION(this);
  m_peersDownloadSpeeds = peersDownloadSpeeds;
}

void BitcoinNode::SetPeersUploadSpeeds(const std::map<Ipv4Address, double> &peersUploadSpeeds)
{
  NS_LOG_FUNCTION(this);
  m_peersUploadSpeeds = peersUploadSpeeds;
}

void BitcoinNode::SetNodeInternetSpeeds(const nodeInternetSpeeds &internetSpeeds)
{
  NS_LOG_FUNCTION(this);

  m_downloadSpeed = internetSpeeds.downloadSpeed * 1000000 / 8;
  m_uploadSpeed = internetSpeeds.uploadSpeed * 1000000 / 8;
}

void BitcoinNode::SetNodeStats(nodeStatistics *nodeStats)
{
  NS_LOG_FUNCTION(this);
  m_nodeStats = nodeStats;
}

void BitcoinNode::SetProtocolType(enum ProtocolType protocolType)
{
  NS_LOG_FUNCTION(this);
  m_protocolType = protocolType;
}

void BitcoinNode::DoDispose(void)
{
  NS_LOG_FUNCTION(this);
  m_socket = 0;

  // chain up
  Application::DoDispose();
}

// Application Methods
void BitcoinNode::StartApplication() // Called at time specified by Start
{
  NS_LOG_FUNCTION(this);
  // Create the socket if not already

  srand(time(NULL) + GetNode()->GetId());
  // NS_LOG_INFO ("Node " << GetNode()->GetId() << ": download speed = " << m_downloadSpeed << " B/s");
  // NS_LOG_INFO ("Node " << GetNode()->GetId() << ": upload speed = " << m_uploadSpeed << " B/s");
  // NS_LOG_INFO ("Node " << GetNode()->GetId() << ": m_numberOfPeers = " << m_numberOfPeers);
  // NS_LOG_INFO ("Node " << GetNode()->GetId() << ": m_invTimeoutMinutes = " << m_invTimeoutMinutes.GetMinutes() << "mins");
  // NS_LOG_WARN ("Node " << GetNode()->GetId() << ": m_protocolType = " << getProtocolType(m_protocolType));
  // NS_LOG_WARN ("Node " << GetNode()->GetId() << ": m_blockTorrent = " << m_blockTorrent);
  // NS_LOG_WARN ("Node " << GetNode()->GetId() << ": m_chunkSize = " << m_chunkSize << " Bytes");
  //
  // NS_LOG_INFO ("Node " << GetNode()->GetId() << ": My peers are");

  // for (auto it = m_peersAddresses.begin(); it != m_peersAddresses.end(); it++)
  //   NS_LOG_INFO("\t" << *it);

  double currentMax = 0;

  //for(auto it = m_peersDownloadSpeeds.begin(); it != m_peersDownloadSpeeds.end(); ++it )
  //{
  //  //std::cout << "Node " << GetNode()->GetId() << ": peer " << it->first << "download speed = " << it->second << " Mbps" << std::endl;
  //}

  if (!m_socket) //if socket has not been assigned
  {
    m_socket = Socket::CreateSocket(GetNode(), m_tid);
    m_socket->Bind(m_local);
    m_socket->Listen();
    m_socket->ShutdownSend();
    if (addressUtils::IsMulticast(m_local))
    {
      Ptr<UdpSocket> udpSocket = DynamicCast<UdpSocket>(m_socket);
      if (udpSocket)
      {
        // equivalent to setsockopt (MCAST_JOIN_GROUP)
        udpSocket->MulticastJoinGroup(0, m_local);
      }
      else
      {
        NS_FATAL_ERROR("Error: joining multicast on a non-UDP socket");
      }
    }
  }

  m_socket->SetRecvCallback(MakeCallback(&BitcoinNode::HandleRead, this)); //Class method member pointer+Class instance
  m_socket->SetAcceptCallback(
      MakeNullCallback<bool, Ptr<Socket>, const Address &>(),
      MakeCallback(&BitcoinNode::HandleAccept, this));
  m_socket->SetCloseCallbacks(
      MakeCallback(&BitcoinNode::HandlePeerClose, this),
      MakeCallback(&BitcoinNode::HandlePeerError, this));

  // NS_LOG_DEBUG ("Node " << GetNode()->GetId() << ": Before creating sockets");
  for (std::vector<Ipv4Address>::const_iterator i = m_peersAddresses.begin(); i != m_peersAddresses.end(); ++i)
  {
    m_peersSockets[*i] = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
    m_peersSockets[*i]->Connect(InetSocketAddress(*i, m_bitcoinPort));
  }
  // NS_LOG_DEBUG ("Node " << GetNode()->GetId() << ": After creating sockets");

  m_nodeStats->nodeId = GetNode()->GetId();
  m_nodeStats->meanBlockReceiveTime = 0;
  m_nodeStats->meanBlockPropagationTime = 0;
  m_nodeStats->meanBlockSize = 0;
  m_nodeStats->totalBlocks = 0;
  m_nodeStats->staleBlocks = 0;
  m_nodeStats->miner = 0;
  m_nodeStats->minerGeneratedBlocks = 0;
  m_nodeStats->minerAverageBlockGenInterval = 0;
  m_nodeStats->minerAverageBlockSize = 0;
  m_nodeStats->hashRate = 0;
  m_nodeStats->attackSuccess = 0;
  m_nodeStats->invReceivedBytes = 0;
  m_nodeStats->invSentBytes = 0;
  m_nodeStats->getHeadersReceivedBytes = 0;
  m_nodeStats->getHeadersSentBytes = 0;
  m_nodeStats->headersReceivedBytes = 0;
  m_nodeStats->headersSentBytes = 0;
  m_nodeStats->getDataReceivedBytes = 0;
  m_nodeStats->getDataSentBytes = 0;
  m_nodeStats->blockReceivedBytes = 0;
  m_nodeStats->blockSentBytes = 0;
  m_nodeStats->longestFork = 0;
  m_nodeStats->blocksInForks = 0;
  m_nodeStats->connections = m_peersAddresses.size();
  m_nodeStats->blockTimeouts = 0;
  m_nodeStats->minedBlocksInMainChain = 0;
  m_nodeStats->transactionReceivedBytes = 0;
  m_nodeStats->transactionSentBytes = 0;
  m_nodeStats->compactBlockReceivedBytes = 0;
  m_nodeStats->compactBlockSentBytes = 0;
  m_nodeStats->getblocktxnReceivedBytes = 0;
  m_nodeStats->getblocktxnSentBytes = 0;
  m_nodeStats->blocktxnReceivedBytes = 0;
  m_nodeStats->blocktxnSentBytes = 0;
  m_nodeStats->invTxnReceivedBytes = 0;     // invtxn Received Bytes
  m_nodeStats->invTxnSentBytes = 0;         //invtxn Received Bytes
  m_nodeStats->getDataTxnReceivedBytes = 0; // getdatatxn Received Bytes
  m_nodeStats->getDataTxnSentBytes = 0;     //getdatatxn Received Bytes
  GenerateTransactions();
}

void BitcoinNode::StopApplication() // Called at time specified by Stop
{
  NS_LOG_FUNCTION(this);

  Simulator::Cancel(m_nextTransactionGenerationEvent);
  for (std::vector<Ipv4Address>::iterator i = m_peersAddresses.begin(); i != m_peersAddresses.end(); ++i) //close the outgoing sockets
  {
    m_peersSockets[*i]->Close();
  }

  if (m_socket)
  {
    m_socket->Close();
    m_socket->SetRecvCallback(MakeNullCallback<void, Ptr<Socket>>());
  }

  // NS_LOG_WARN ("\n\nBitcoin NODE " << GetNode ()->GetId () << ":");
  // NS_LOG_WARN ("Current Top Block is:\n" << *(m_blockchain.GetCurrentTopBlock()));
  // NS_LOG_WARN ("Current Blockchain is:\n" << m_blockchain);
  //m_blockchain.PrintOrphans();
  //PrintQueueInv();
  //PrintInvTimeouts();

  // NS_LOG_WARN("Mean Block Receive Time = " << m_meanBlockReceiveTime << " or "
  //              << static_cast<int>(m_meanBlockReceiveTime) / m_secondsPerMin << "min and "
  //              << m_meanBlockReceiveTime - static_cast<int>(m_meanBlockReceiveTime) / m_secondsPerMin * m_secondsPerMin << "s");
  // NS_LOG_WARN("Mean Block Propagation Time = " << m_meanBlockPropagationTime << "s");
  // NS_LOG_WARN("Mean Block Size = " << m_meanBlockSize << " Bytes");
  // NS_LOG_WARN("Total Blocks = " << m_blockchain.GetTotalBlocks());
  // NS_LOG_WARN("Stale Blocks = " << m_blockchain.GetNoStaleBlocks() << " ("
  //             << 100. * m_blockchain.GetNoStaleBlocks() / m_blockchain.GetTotalBlocks() << "%)");
  // NS_LOG_WARN("receivedButNotValidated size = " << m_receivedNotValidated.size());
  // NS_LOG_WARN("m_sendQueueTimes size = " << m_sendQueueTimes.size());
  // NS_LOG_WARN("m_receiveQueueTimes size = " << m_receiveQueueTimes.size());
  // NS_LOG_WARN("longest fork = " << m_blockchain.GetLongestForkSize());
  // NS_LOG_WARN("blocks in forks = " << m_blockchain.GetBlocksInForks());

  m_nodeStats->meanBlockReceiveTime = m_meanBlockReceiveTime;
  m_nodeStats->meanBlockPropagationTime = m_meanBlockPropagationTime;
  m_nodeStats->meanBlockSize = m_meanBlockSize;
  m_nodeStats->totalBlocks = m_blockchain.GetTotalBlocks();
  m_nodeStats->staleBlocks = m_blockchain.GetNoStaleBlocks();
  m_nodeStats->longestFork = m_blockchain.GetLongestForkSize();
  m_nodeStats->blocksInForks = m_blockchain.GetBlocksInForks();
}

void BitcoinNode::HandleRead(Ptr<Socket> socket)
{
  // NS_LOG_FUNCTION(this);
  Ptr<Packet> packet;
  Address from;
  double newBlockReceiveTime = Simulator::Now().GetSeconds();

  while ((packet = socket->RecvFrom(from)))
  {
    if (packet->GetSize() == 0)
    { //EOF
      break;
    }

    if (InetSocketAddress::IsMatchingType(from)) //Test whether the address is IPV4 or not --Lihao
    {
      /**
         * We may receive more than one packets simultaneously on the socket,
         * so we have to parse each one of them.
         */
      std::string delimiter = "#";
      std::string parsedPacket;
      size_t pos = 0;
      char *packetInfo = new char[packet->GetSize() + 1];
      std::ostringstream totalStream;

      packet->CopyData(reinterpret_cast<uint8_t *>(packetInfo), packet->GetSize());
      packetInfo[packet->GetSize()] = '\0'; // ensure that it is null terminated to avoid bugs

      /**
         * Add the buffered data to complete the packet
         */
      totalStream << m_bufferedData[from] << packetInfo;
      std::string totalReceivedData(totalStream.str());
      //std::cout << "Node " << GetNode()->GetId() << " Total Received Data: " << totalReceivedData << std::endl;

      while ((pos = totalReceivedData.find(delimiter)) != std::string::npos)
      {
        parsedPacket = totalReceivedData.substr(0, pos);

        //std::cout << "Node " << GetNode()->GetId() << " Parsed Packet: " << parsedPacket << std::endl;

        rapidjson::Document d;
        d.Parse(parsedPacket.c_str());

        if (!d.IsObject())
        {
          std::cout << "The parsed packet is corrupted" << std::endl;
          //std::cout << "Node " << GetNode()->GetId() << " corrupted Packet: " << parsedPacket << std::endl;
          totalReceivedData.erase(0, pos + delimiter.length());
          continue;
        }

        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        d.Accept(writer);

        NS_LOG_INFO("At time " << Simulator::Now().GetSeconds()
                               << "s bitcoin node " << GetNode()->GetId() << " received "
                               << packet->GetSize() << " bytes from "
                               << InetSocketAddress::ConvertFrom(from).GetIpv4()
                               << " port " << InetSocketAddress::ConvertFrom(from).GetPort()
                               << " with info = " << buffer.GetString());
        
        switch (d["message"].GetInt())
        {
        case INV:
        {
          //NS_LOG_INFO ("INV");
          int j;

          std::vector<std::string> requestBlocks;
          std::vector<std::string>::iterator block_it;
          double invmessageSize = 0;
          double getdataMessageSize = 0;

          invmessageSize = m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes; //++invReceivedBytes;
          m_nodeStats->invReceivedBytes += invmessageSize;

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = invmessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          for (j = 0; j < d["inv"].Size(); j++)
          {
            std::string invDelimiter = "/";
            std::string parsedInv = d["inv"][j].GetString();
            size_t invPos = parsedInv.find(invDelimiter);
            EventId timeout;

            int height = atoi(parsedInv.substr(0, invPos).c_str());
            int minerId = atoi(parsedInv.substr(invPos + 1, parsedInv.size()).c_str());

            if (m_blockchain.HasBlock(height, minerId) || m_blockchain.IsOrphan(height, minerId) || ReceivedButNotValidated(parsedInv))
            {
              NS_LOG_INFO("INV: Bitcoin node " << GetNode()->GetId()
                                               << " has already received the block with height = "
                                               << height << " and minerId = " << minerId);
            }
            else
            {
              NS_LOG_INFO("INV: Bitcoin node " << GetNode()->GetId()
                                               << " does not have the block with height = "
                                               << height << " and minerId = " << minerId);

              /**
                   * Check if we have already requested the block
                   */

              if (m_invTimeouts.find(parsedInv) == m_invTimeouts.end())
              {
                NS_LOG_INFO("INV: Bitcoin node " << GetNode()->GetId()
                                                 << " has not requested the block yet");
                requestBlocks.push_back(parsedInv);
                timeout = Simulator::Schedule(m_invTimeoutMinutes, &BitcoinNode::InvTimeoutExpired, this, parsedInv);
                m_invTimeouts[parsedInv] = timeout;
              }
              else
              {
                NS_LOG_INFO("INV: Bitcoin node " << GetNode()->GetId()
                                                 << " has already requested the block");
              }

              m_queueInv[parsedInv].push_back(from);
            }
          }

          if (!requestBlocks.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);
            d.RemoveMember("inv");

            for (block_it = requestBlocks.begin(); block_it < requestBlocks.end(); block_it++)
            {
              value.SetString(block_it->c_str(), block_it->size(), d.GetAllocator());
              array.PushBack(value, d.GetAllocator());
            }
            d.AddMember("blocks", array, d.GetAllocator());

            rapidjson::StringBuffer inv;
            rapidjson::Writer<rapidjson::StringBuffer> invWriter(inv);
            d.Accept(invWriter);
            std::string help = inv.GetString();

            getdataMessageSize = m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
            Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendGetData, this, help, getdataMessageSize, from);
          }
          break;
        }
        case INV_TXN:
        {
          //NS_LOG_INFO ("INV");
          double invTxnmessageSize = 0;
          double getDataTxnMessageSize = 0;

          invTxnmessageSize = m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
          m_nodeStats->invTxnReceivedBytes += invTxnmessageSize;

          std::string transactionShortHash = d["shortHash"].GetString();

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = invTxnmessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          if (!m_mempool.HasShortTransaction(transactionShortHash) && !m_blockchain.HasTransaction(transactionShortHash))
          {
            rapidjson::StringBuffer invTxn;
            rapidjson::Writer<rapidjson::StringBuffer> invTxnWriter(invTxn);
            d.Accept(invTxnWriter);
            std::string help = invTxn.GetString();

            getDataTxnMessageSize = m_bitcoinMessageHeader + m_countBytes +  m_inventorySizeBytes;
            Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendGetDataTxn, this, help, getDataTxnMessageSize,from);
          }
          break;
        }
        case GET_HEADERS:
        {
          int j;
          std::vector<Block> requestHeaders;
          std::vector<Block>::iterator block_it;

          m_nodeStats->getHeadersReceivedBytes += m_bitcoinMessageHeader + m_getHeadersSizeBytes;

          for (j = 0; j < d["blocks"].Size(); j++)
          {
            std::string invDelimiter = "/";
            std::string blockHash = d["blocks"][j].GetString();
            size_t invPos = blockHash.find(invDelimiter);

            int height = atoi(blockHash.substr(0, invPos).c_str());
            int minerId = atoi(blockHash.substr(invPos + 1, blockHash.size()).c_str());

            if (m_blockchain.HasBlock(height, minerId) || m_blockchain.IsOrphan(height, minerId))
            {
              NS_LOG_INFO("GET_HEADERS: Bitcoin node " << GetNode()->GetId()
                                                       << " has the block with height = "
                                                       << height << " and minerId = " << minerId);
              Block newBlock(m_blockchain.ReturnBlock(height, minerId));
              requestHeaders.push_back(newBlock);
            }
            else if (ReceivedButNotValidated(blockHash))
            {
              NS_LOG_INFO("GET_HEADERS: Bitcoin node " << GetNode()->GetId()
                                                       << " has received but not yet validated the block with height = "
                                                       << height << " and minerId = " << minerId);
              requestHeaders.push_back(m_receivedNotValidated[blockHash]);
            }
            else
            {
              NS_LOG_INFO("GET_HEADERS: Bitcoin node " << GetNode()->GetId()
                                                       << " does not have the full block with height = "
                                                       << height << " and minerId = " << minerId);
            }
          }

          if (!requestHeaders.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);

            d.RemoveMember("blocks");

            for (block_it = requestHeaders.begin(); block_it < requestHeaders.end(); block_it++)
            {
              rapidjson::Value blockInfo(rapidjson::kObjectType);
              NS_LOG_INFO("In requestHeaders " << *block_it);

              value = block_it->GetBlockHeight();
              blockInfo.AddMember("height", value, d.GetAllocator());

              value = block_it->GetMinerId();
              blockInfo.AddMember("minerId", value, d.GetAllocator());

              value = block_it->GetParentBlockMinerId();
              blockInfo.AddMember("parentBlockMinerId", value, d.GetAllocator());

              value = block_it->GetBlockSizeBytes();
              blockInfo.AddMember("size", value, d.GetAllocator());

              value = block_it->GetTimeCreated();
              blockInfo.AddMember("timeCreated", value, d.GetAllocator());

              value = block_it->GetTimeReceived();
              blockInfo.AddMember("timeReceived", value, d.GetAllocator());

              array.PushBack(blockInfo, d.GetAllocator());
            }

            d.AddMember("blocks", array, d.GetAllocator());

            SendMessage_2(GET_HEADERS, HEADERS, d, from);
          }
          break;
        }
        case HEADERS:
        {
          NS_LOG_INFO("HEADERS");

          std::vector<std::string> requestHeaders;
          std::vector<std::string> requestBlocks;
          std::vector<std::string>::iterator block_it;
          int j;

          m_nodeStats->headersReceivedBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;

          for (j = 0; j < d["blocks"].Size(); j++)
          {
            int parentHeight = d["blocks"][j]["height"].GetInt() - 1;
            int parentMinerId = d["blocks"][j]["parentBlockMinerId"].GetInt();
            int height = d["blocks"][j]["height"].GetInt();
            int minerId = d["blocks"][j]["minerId"].GetInt();
            // Since it is a header, we assume here that it don't have any information related to transaction. -Lihao
            int transactionCount = 0;
            std::map<std::string, Transaction> blockTransactions = {{"shortHash", Transaction(0, "defaultHash", "shortHash")}};

            EventId timeout;
            std::ostringstream stringStream;
            std::string blockHash;
            std::string parentBlockHash;

            stringStream << height << "/" << minerId;
            blockHash = stringStream.str();
            //Block newBlockHeaders(d["blocks"][j]["height"].GetInt(), d["blocks"][j]["minerId"].GetInt(), d["blocks"][j]["parentBlockMinerId"].GetInt(),
            //d["blocks"][j]["size"].GetInt(), d["blocks"][j]["timeCreated"].GetDouble(),
            //Simulator::Now().GetSeconds(), transactionCount, blockTransactions, InetSocketAddress::ConvertFrom(from).GetIpv4());

            // Add it to the header chain if we don't have it before -Lihao
            if (!OnlyHeadersReceived(blockHash))
            {
              m_onlyHeadersReceived[blockHash] = Block(d["blocks"][j]["height"].GetInt(), d["blocks"][j]["minerId"].GetInt(), d["blocks"][j]["parentBlockMinerId"].GetInt(),
                                                       d["blocks"][j]["size"].GetInt(), d["blocks"][j]["timeCreated"].GetDouble(),
                                                       Simulator::Now().GetSeconds(), transactionCount, blockTransactions, InetSocketAddress::ConvertFrom(from).GetIpv4());
            }
            //PrintOnlyHeadersReceived();

            stringStream.clear();
            stringStream.str("");

            stringStream << parentHeight << "/" << parentMinerId;
            parentBlockHash = stringStream.str();

            if (m_protocolType == SENDHEADERS && !m_blockchain.HasBlock(height, minerId) && !m_blockchain.IsOrphan(height, minerId) && !ReceivedButNotValidated(blockHash))
            {
              NS_LOG_INFO("We have not received an INV for the block with height = " << d["blocks"][j]["height"].GetInt()
                                                                                     << " and minerId = " << d["blocks"][j]["minerId"].GetInt());

              /**
                   * Acquire block
                   */

              if (m_invTimeouts.find(blockHash) == m_invTimeouts.end())
              {
                NS_LOG_INFO("HEADERS: Bitcoin node " << GetNode()->GetId()
                                                     << " has not requested the block yet");
                requestBlocks.push_back(blockHash.c_str());
                timeout = Simulator::Schedule(m_invTimeoutMinutes, &BitcoinNode::InvTimeoutExpired, this, blockHash);
                m_invTimeouts[blockHash] = timeout;
              }
              else
              {
                NS_LOG_INFO("HEADERS: Bitcoin node " << GetNode()->GetId()
                                                     << " has already requested the block");
              }

              m_queueInv[blockHash].push_back(from);
            }

            if (!m_blockchain.HasBlock(parentHeight, parentMinerId) && !m_blockchain.IsOrphan(parentHeight, parentMinerId) && !ReceivedButNotValidated(parentBlockHash))
            {
              NS_LOG_INFO("The Block with height = " << d["blocks"][j]["height"].GetInt()
                                                     << " and minerId = " << d["blocks"][j]["minerId"].GetInt()
                                                     << " is an orphan\n");

              /**
                   * Acquire parent
                   */

              if (m_invTimeouts.find(parentBlockHash) == m_invTimeouts.end())
              {
                NS_LOG_INFO("HEADERS: Bitcoin node " << GetNode()->GetId()
                                                     << " has not requested its parent block yet");

                if (m_protocolType == STANDARD_PROTOCOL ||
                    (m_protocolType == SENDHEADERS && std::find(requestBlocks.begin(), requestBlocks.end(), parentBlockHash) == requestBlocks.end()))
                {
                  if (!OnlyHeadersReceived(parentBlockHash))
                    requestHeaders.push_back(parentBlockHash.c_str());
                  timeout = Simulator::Schedule(m_invTimeoutMinutes, &BitcoinNode::InvTimeoutExpired, this, parentBlockHash);
                  m_invTimeouts[parentBlockHash] = timeout;
                }
              }
              else
              {
                NS_LOG_INFO("HEADERS: Bitcoin node " << GetNode()->GetId()
                                                     << " has already requested the block");
              }

              if (m_protocolType == STANDARD_PROTOCOL ||
                  (m_protocolType == SENDHEADERS && std::find(requestBlocks.begin(), requestBlocks.end(), parentBlockHash) == requestBlocks.end()))
                m_queueInv[parentBlockHash].push_back(from);

              //PrintQueueInv();
              //PrintInvTimeouts();
            }
            else
            {
              /**
	               * Block is not orphan, so we can go on validating
	               */
              NS_LOG_INFO("The Block with height = " << d["blocks"][j]["height"].GetInt()
                                                     << " and minerId = " << d["blocks"][j]["minerId"].GetInt()
                                                     << " is NOT an orphan\n");
            }
          }

          if (!requestHeaders.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);
            Time timeout;

            d.RemoveMember("blocks");

            for (block_it = requestHeaders.begin(); block_it < requestHeaders.end(); block_it++)
            {
              value.SetString(block_it->c_str(), block_it->size(), d.GetAllocator());
              array.PushBack(value, d.GetAllocator());
            }

            d.AddMember("blocks", array, d.GetAllocator());

            SendMessage_2(HEADERS, GET_HEADERS, d, from);
            SendMessage_2(HEADERS, GET_DATA, d, from);
          }

          if (!requestBlocks.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);
            Time timeout;

            d.RemoveMember("blocks");

            for (block_it = requestBlocks.begin(); block_it < requestBlocks.end(); block_it++)
            {
              value.SetString(block_it->c_str(), block_it->size(), d.GetAllocator());
              array.PushBack(value, d.GetAllocator());
            }

            d.AddMember("blocks", array, d.GetAllocator());

            SendMessage_2(HEADERS, GET_DATA, d, from);
          }
          break;
        }
        case GET_DATA:
        {
          NS_LOG_INFO("GET_DATA");

          int j;
          double getdataMessageSize = 0;
          std::vector<Block> requestBlocks;
          std::vector<Block>::iterator block_it;

          getdataMessageSize = m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
          m_nodeStats->getDataReceivedBytes += getdataMessageSize;

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = getdataMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          for (j = 0; j < d["blocks"].Size(); j++)
          {
            std::string invDelimiter = "/";
            std::string parsedInv = d["blocks"][j].GetString();
            size_t invPos = parsedInv.find(invDelimiter);

            int height = atoi(parsedInv.substr(0, invPos).c_str());
            int minerId = atoi(parsedInv.substr(invPos + 1, parsedInv.size()).c_str());

            if (m_blockchain.HasBlock(height, minerId))
            {
              NS_LOG_INFO("GET_DATA: Bitcoin node " << GetNode()->GetId()
                                                    << " has already received the block with height = "
                                                    << height << " and minerId = " << minerId);
              Block newBlock(m_blockchain.ReturnBlock(height, minerId));
              requestBlocks.push_back(newBlock);
            }
            else
            {
              NS_LOG_INFO("GET_DATA: Bitcoin node " << GetNode()->GetId()
                                                    << " does not have the block with height = "
                                                    << height << " and minerId = " << minerId);
            }
          }

          int transactionCount = 0;
          double blockMessageSize = 0;
          double compactblockMessageSize = 0;

          if (!requestBlocks.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);

            d.RemoveMember("blocks");

            for (block_it = requestBlocks.begin(); block_it < requestBlocks.end(); block_it++)
            {
              rapidjson::Value blockInfo(rapidjson::kObjectType);
              NS_LOG_INFO("In requestBlocks " << *block_it);

              value = block_it->GetBlockHeight();
              blockInfo.AddMember("height", value, d.GetAllocator());

              value = block_it->GetMinerId();
              blockInfo.AddMember("minerId", value, d.GetAllocator());

              value = block_it->GetParentBlockMinerId();
              blockInfo.AddMember("parentBlockMinerId", value, d.GetAllocator());

              value = block_it->GetBlockSizeBytes();
              blockMessageSize += value.GetInt();
              blockInfo.AddMember("size", value, d.GetAllocator());

              value = block_it->GetTimeCreated();
              blockInfo.AddMember("timeCreated", value, d.GetAllocator());

              value = block_it->GetTimeReceived();
              blockInfo.AddMember("timeReceived", value, d.GetAllocator());

              value = block_it->GetTransactionCount();
              transactionCount += block_it->GetTransactionCount();
              blockInfo.AddMember("transactionCount", value, d.GetAllocator());

              rapidjson::Value transactionArray(rapidjson::kArrayType);

              std::map<std::string, Transaction> thisBlockTransactions = block_it->GetBlockTransactions();

              for (std::map<std::string, Transaction>::const_iterator it = thisBlockTransactions.begin(); it != thisBlockTransactions.end(); it++)
              {
                rapidjson::Value transactionInfo(rapidjson::kObjectType);

                value.SetString(((it->second).GetTransactionShortHash()).c_str(), ((it->second).GetTransactionShortHash()).length(), d.GetAllocator());
                transactionInfo.AddMember("transactionShortHash", value, d.GetAllocator());

                value.SetString(((it->second).GetTransactionHash()).c_str(), ((it->second).GetTransactionHash()).length(), d.GetAllocator());
                transactionInfo.AddMember("transactionHash", value, d.GetAllocator());

                value = (it->second).GetTransactionSizeBytes();
                transactionInfo.AddMember("transactionSizeBytes", value, d.GetAllocator());

                transactionArray.PushBack(transactionInfo, d.GetAllocator());
              }
              blockInfo.AddMember("transactions", transactionArray, d.GetAllocator());

              array.PushBack(blockInfo, d.GetAllocator());
            }
            d.AddMember("blocks", array, d.GetAllocator());

            // Stringify the DOM
            rapidjson::StringBuffer packetInfo;
            rapidjson::Writer<rapidjson::StringBuffer> writer(packetInfo);
            d.Accept(writer);
            std::string packet = packetInfo.GetString();
            NS_LOG_INFO("DEBUG: " << packetInfo.GetString());

            if (m_protocolType == COMPACT)
            {
              compactblockMessageSize = m_bitcoinMessageHeader + (transactionCount * 6) + 8 + 1 + 250;
              Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendBlock, this, packet, compactblockMessageSize, from);
            }
            else
            {
              Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendBlock, this, packet, blockMessageSize, from);
            }
          }
          break;
        }
        case GET_DATA_TXN:
        {
          NS_LOG_INFO("GET_DATA");

          double getDataMessageSize = 0;
          getDataMessageSize = m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
          m_nodeStats->getDataTxnReceivedBytes += getDataMessageSize;

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = getDataMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          std::string transactionShortHash = d["shortHash"].GetString();
          double transactionSizeBytes = d["size"].GetDouble();

          if (m_mempool.HasShortTransaction(transactionShortHash) || m_blockchain.HasTransaction(transactionShortHash))
          {
            // Stringify the DOM
            rapidjson::StringBuffer packetInfo;
            rapidjson::Writer<rapidjson::StringBuffer> writer(packetInfo);
            d.Accept(writer);
            std::string packet = packetInfo.GetString();

            double txnMessageSize = m_bitcoinMessageHeader + transactionSizeBytes;
            Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendTransaction, this, packet, txnMessageSize, from);
          }
          break;
        }
        case BLOCK:
        {
          NS_LOG_INFO("BLOCK");
          int blockMessageSize = 0;
          std::string blockType = d["type"].GetString();

          blockMessageSize += m_bitcoinMessageHeader;

          for (int j = 0; j < d["blocks"].Size(); j++)
          {
            blockMessageSize += d["blocks"][j]["size"].GetInt();
          }

          m_nodeStats->blockReceivedBytes += blockMessageSize;

          double receiveTime = 0;
          double eventTime = 0;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = blockMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          // Stringify the DOM
          rapidjson::StringBuffer blockInfo;
          rapidjson::Writer<rapidjson::StringBuffer> blockWriter(blockInfo);
          d.Accept(blockWriter);

          NS_LOG_INFO("BLOCK: At time " << Simulator::Now().GetSeconds()
                                        << " Node " << GetNode()->GetId() << " received a block message " << blockInfo.GetString());
          NS_LOG_INFO(m_downloadSpeed << " " << m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8 << " " << minSpeed);

          std::string help = blockInfo.GetString();

          Simulator::Schedule(Seconds(eventTime), &BitcoinNode::ReceivedBlockMessage, this, help, from);
          NS_LOG_INFO("BLOCK:  Node " << GetNode()->GetId() << " will receive the full block message at " << Simulator::Now().GetSeconds() + eventTime);

          break;
        }
        case TXN:
        {
          // NS_LOG_INFO("TXN");
          //std::cout << "Receive txn" << std::endl;
          double txnmessageSize = 0;
          rapidjson::StringBuffer transaction;
          rapidjson::Writer<rapidjson::StringBuffer> transactionWriter(transaction);
          d.Accept(transactionWriter);
          std::string help = transaction.GetString();

          double transactionSizeBytes = d["size"].GetDouble();
          std::string transactionHash = d["hash"].GetString();
          std::string transactionShortHash = d["shortHash"].GetString();

          txnmessageSize = m_bitcoinMessageHeader + transactionSizeBytes;
          m_nodeStats->transactionReceivedBytes += txnmessageSize;

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = txnmessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          // Relay to peers beside the "from" only when the node donnot has this transaction(Both in mempool and blockchain) --Lihao (TODO: Identify by long transactionHash)
          if (!m_mempool.HasShortTransaction(transactionShortHash) && !m_blockchain.HasTransaction(transactionShortHash))
          {
            Transaction newTransaction(transactionSizeBytes, transactionHash, transactionShortHash);
            //std::cout << "Add Transaction: At Time " << Simulator::Now().GetSeconds() << std::endl;

            m_mempool.AddTransaction(newTransaction);
            Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendInvTxn, this, help, from);
          }
          break;
        }
        case COMPACT_BLOCK:
        {
          NS_LOG_FUNCTION(this);

          NS_LOG_INFO("COMPACT BLOCK");
          double compctblockMessageSize = 0;
          std::vector<Block> requestBlocks;
          std::vector<Block>::iterator block_it;

          for (int i = 0; i < d["blocks"].Size(); i++)
          {
            int transactionCount = d["blocks"][i]["transactions"].Size();
            compctblockMessageSize += (transactionCount * 6) + 8 + 1 + 250;

            int height = d["blocks"][i]["height"].GetInt();
            int minerId = d["blocks"][i]["minerId"].GetInt();

            if (m_blockchain.HasBlock(height, minerId))
            {
              NS_LOG_INFO("COMPACT_BLOCK: Bitcoin node " << GetNode()->GetId() << " has already received the block height = "
                                                         << height << " and minerId = " << minerId);
            }
            else
            {
              NS_LOG_INFO("COMPACT_BLOCK: Bitcoin node " << GetNode()->GetId() << " does not have the block with height = "
                                                         << height << " and minerId = " << minerId);
              Block newBlock(m_blockchain.ReturnBlock(height, minerId));
              requestBlocks.push_back(newBlock);
            }
          }
          compctblockMessageSize += m_bitcoinMessageHeader;
          m_nodeStats->compactBlockReceivedBytes += compctblockMessageSize;

          //Stringify the DOM
          rapidjson::StringBuffer blockInfo;
          rapidjson::Writer<rapidjson::StringBuffer> blockWriter(blockInfo);
          d.Accept(blockWriter);

          std::string help = blockInfo.GetString();

          // NS_LOG_INFO ("help is : " << help << std::endl);
          double receiveTime = 0;
          double eventTime = 0;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = compctblockMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          if (!requestBlocks.empty())
          {
            // d.RemoveMember("blocks");

            bool isMissingTransactions = false;
            rapidjson::Value missingTransactionArray(rapidjson::kArrayType);

            std::map<std::string, Transaction> blockTransactions;
            int transactionCount = d["blocks"][0]["transactions"].Size();

            for (int i = 0; i < transactionCount; i++)
            {
              std::string transactionShortHash = d["blocks"][0]["transactions"][i]["transactionShortHash"].GetString();
              std::string transactionHash = d["blocks"][0]["transactions"][i]["transactionHash"].GetString();
              double transactionSizeBytes = d["blocks"][0]["transactions"][i]["transactionSizeBytes"].GetDouble();

              Transaction newTransaction(transactionSizeBytes, transactionHash, transactionShortHash);
              blockTransactions.insert({transactionShortHash, newTransaction});

              if (!m_mempool.HasShortTransaction(transactionShortHash))
              {
                int transactionShortHashSize = transactionShortHash.size();
                int transactionHashSize = transactionHash.size();

                rapidjson::Value value;
                rapidjson::Value missingTransactionInfo(rapidjson::kObjectType);

                value.SetString(transactionShortHash.c_str(), transactionShortHashSize, d.GetAllocator());
                missingTransactionInfo.AddMember("transactionShortHash", value, d.GetAllocator());

                value.SetString(transactionHash.c_str(), transactionHashSize, d.GetAllocator());
                missingTransactionInfo.AddMember("transactionHash", value, d.GetAllocator());

                value = transactionSizeBytes;
                missingTransactionInfo.AddMember("transactionSizeBytes", value, d.GetAllocator());

                missingTransactionArray.PushBack(missingTransactionInfo, d.GetAllocator());

                isMissingTransactions = true;
              }
            }
            d.AddMember("missingTransactions", missingTransactionArray, d.GetAllocator());
            //std::cout << "isMissingTransactions" <<isMissingTransactions <<std::endl;

            if (isMissingTransactions)
            {
              int missingTransactionCount = d["missingTransactions"].Size();
              double getblocktxnsMessageSize = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length

              // Need to request the missing transactions -Lihao
              // Stringify the DOM
              rapidjson::StringBuffer blockInfo;
              rapidjson::Writer<rapidjson::StringBuffer> blockWriter(blockInfo);
              d.Accept(blockWriter);
              std::string help = blockInfo.GetString();

              Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendGetBlockTransactions, this, help, getblocktxnsMessageSize,from);
            }

            else // This block can be completed from mempool.
            {
              NS_LOG_INFO("COMPLETE COMPACT BLOCK");

              Simulator::Schedule(Seconds(eventTime), &BitcoinNode::ReceivedBlockMessage, this, help, from);

              NS_LOG_INFO("COMPACT BLOCK:  Node " << GetNode()->GetId() << " will receive the compact block message after: " << eventTime);
            }
          }
          break;
        }
        case GET_BLOCK_TXNS:
        {
          NS_LOG_INFO("GET_BLOCK_TXNS");

          int missingTransactionCount = d["missingTransactions"].Size();
          int getblocktxnsMessageSize = 0;
          std::vector<Block> requestBlocks;
          std::vector<Block>::iterator block_it;
          // double transactionSize = 0;
          double blockTransactionsMissingRequest = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length
          getblocktxnsMessageSize = m_bitcoinMessageHeader + blockTransactionsMissingRequest;
          m_nodeStats->getblocktxnReceivedBytes += getblocktxnsMessageSize;

          double receiveTime;
          double eventTime;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = getblocktxnsMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          for (int i = 0; i < d["blocks"].Size(); i++)
          {
            int height = d["blocks"][i]["height"].GetInt();
            int minerId = d["blocks"][i]["minerId"].GetInt();

            if (m_blockchain.HasBlock(height, minerId))
            {
              NS_LOG_INFO("GET_BLOCK_TXNS: Bitcoin node " << GetNode()->GetId()
                                                       << " has already received the block with height = "
                                                       << height << " and minerId = " << minerId);

              Block newBlock(m_blockchain.ReturnBlock(height, minerId));
              requestBlocks.push_back(newBlock);
            }
            else
            {
              NS_LOG_INFO("GET_BLOCK_TXNS: Bitcoin node " << GetNode()->GetId()
                                                       << " does not have the block with height = "
                                                       << height << " and minerId = " << minerId);
            }
          }

          if (!requestBlocks.empty())
          {
            rapidjson::Value value;
            rapidjson::Value array(rapidjson::kArrayType);

            d.RemoveMember("blocks");

            for (block_it = requestBlocks.begin(); block_it < requestBlocks.end(); block_it++)
            {

              rapidjson::Value blockInfo(rapidjson::kObjectType);
              NS_LOG_INFO("In requestBlocks " << *block_it);

              value = block_it->GetBlockHeight();
              blockInfo.AddMember("height", value, d.GetAllocator());

              value = block_it->GetMinerId();
              blockInfo.AddMember("minerId", value, d.GetAllocator());

              value = block_it->GetParentBlockMinerId();
              blockInfo.AddMember("parentBlockMinerId", value, d.GetAllocator());

              value = block_it->GetBlockSizeBytes();
              //totalBlockMessageSize += value.GetInt();
              blockInfo.AddMember("size", value, d.GetAllocator());

              value = block_it->GetTransactionCount();
              int transactionCount = block_it->GetTransactionCount();
              blockInfo.AddMember("transactionCount", value, d.GetAllocator());

              value = block_it->GetTimeCreated();
              blockInfo.AddMember("timeCreated", value, d.GetAllocator());

              value = block_it->GetTimeReceived();
              blockInfo.AddMember("timeReceived", value, d.GetAllocator());

              std::map<std::string, Transaction> thisBlockTransactions = block_it->GetBlockTransactions();
              rapidjson::Value transactionArray(rapidjson::kArrayType);

              for (std::map<std::string, Transaction>::const_iterator it = thisBlockTransactions.begin(); it != thisBlockTransactions.end(); it++)
              {
                rapidjson::Value transactionInfo(rapidjson::kObjectType);

                value.SetString(((it->second).GetTransactionShortHash()).c_str(), ((it->second).GetTransactionShortHash()).length(), d.GetAllocator());
                transactionInfo.AddMember("transactionShortHash", value, d.GetAllocator());

                value.SetString(((it->second).GetTransactionHash()).c_str(), ((it->second).GetTransactionHash()).length(), d.GetAllocator());
                transactionInfo.AddMember("transactionHash", value, d.GetAllocator());

                value = (it->second).GetTransactionSizeBytes();
                transactionInfo.AddMember("transactionSizeBytes", value, d.GetAllocator());

                transactionArray.PushBack(transactionInfo, d.GetAllocator());
              }
              blockInfo.AddMember("transactions", transactionArray, d.GetAllocator());

              array.PushBack(blockInfo, d.GetAllocator());
            }

            d.AddMember("blocks", array, d.GetAllocator());


            double missingTransactionSize = 0;
            for (int i = 0; i < d["missingTransactions"].Size(); i++)
            {
              double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
              missingTransactionSize += missingTxSize;
            }
            double blocktxnsMessageSize = m_bitcoinMessageHeader + missingTransactionSize;

            rapidjson::StringBuffer packetInfo;
            rapidjson::Writer<rapidjson::StringBuffer> writer(packetInfo);
            d.Accept(writer);

            std::string packet = packetInfo.GetString();
            Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendBlockTransactions, this, packet, blocktxnsMessageSize,from);
          }

          break;
        }
        case BLOCK_TXNS:
        {
          NS_LOG_INFO("BLOCK_TXNS");

          double blocktxnsMessageSize = 0;
          rapidjson::StringBuffer blockInfo;
          rapidjson::Writer<rapidjson::StringBuffer> blockWriter(blockInfo);
          d.Accept(blockWriter);

          // NS_LOG_INFO("Message Received at block txns : " << blockInfo.GetString() << std::endl);

          double missingTransactionSize = 0;

          for (int i = 0; i < d["missingTransactions"].Size(); i++)
          {
            double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
            missingTransactionSize += missingTxSize;
          }
          blocktxnsMessageSize = m_bitcoinMessageHeader + missingTransactionSize;
          m_nodeStats->blocktxnReceivedBytes += blocktxnsMessageSize;

          double receiveTime = 0;
          double eventTime = 0;
          double minSpeed = std::min(m_downloadSpeed, m_peersUploadSpeeds[InetSocketAddress::ConvertFrom(from).GetIpv4()] * 1000000 / 8);
          receiveTime = blocktxnsMessageSize / minSpeed;
          eventTime = ManageReceiveTime(receiveTime, receiveTime);

          std::string help = blockInfo.GetString();
          Simulator::Schedule(Seconds(eventTime), &BitcoinNode::ReceivedBlockMessage, this, help, from);
          break;
        }

        default:
          NS_LOG_INFO("Default");
          break;
        }
          totalReceivedData.erase(0, pos + delimiter.length());
        }

        /**
        * Buffer the remaining data
        */

        m_bufferedData[from] = totalReceivedData;
        delete[] packetInfo;
      }
      else if (Inet6SocketAddress::IsMatchingType(from))
      {
        NS_LOG_INFO("At time " << Simulator::Now().GetSeconds()
                               << "s bitcoin node " << GetNode()->GetId() << " received "
                               << packet->GetSize() << " bytes from "
                               << Inet6SocketAddress::ConvertFrom(from).GetIpv6()
                               << " port " << Inet6SocketAddress::ConvertFrom(from).GetPort());
      }
      m_rxTrace(packet, from);
    }
  }

  void BitcoinNode::ReceivedBlockMessage(std::string & blockInfo, Address & from)
  {
    NS_LOG_FUNCTION(this);

    rapidjson::Document d;
    d.Parse(blockInfo.c_str());

    //std::cout << "ReceivedBlockMessage: At time " << Simulator::Now().GetSeconds()
    //          << " Node " << GetNode()->GetId() << " received a block message " << blockInfo << std::endl;

    //m_receiveQueueTimes.erase(m_receiveQueueTimes.begin());
    //std::cout << "ReceivedBlockMessage has blocks: " << d["blocks"].Size() << std::endl;

    for (int j = 0; j < d["blocks"].Size(); j++)
    {
      int parentHeight = d["blocks"][j]["height"].GetInt() - 1;
      int parentMinerId = d["blocks"][j]["parentBlockMinerId"].GetInt();
      int height = d["blocks"][j]["height"].GetInt();
      int minerId = d["blocks"][j]["minerId"].GetInt();
      int transactionCount = d["blocks"][j]["transactionCount"].GetInt();

      //std::cout << "ReceivedBlockMessage height is: " << height << " miner is: " << minerId << std::endl;
      //std::cout << "transactionCount: " << d["blocks"][j]["transactions"].Size() << std::endl;

      std::map<std::string, Transaction> blockTransactions;
      // std::map<Transaction>::const_iterator it;

      for (int i = 0; i < d["blocks"][j]["transactions"].Size(); i++)
      {
        double transactionSizeBytes = d["blocks"][j]["transactions"][i]["transactionSizeBytes"].GetDouble();
        std::string transactionHash = d["blocks"][j]["transactions"][i]["transactionHash"].GetString();
        std::string transactionShortHash = d["blocks"][j]["transactions"][i]["transactionShortHash"].GetString();

        Transaction newTransaction(transactionSizeBytes, transactionHash, transactionShortHash);

        blockTransactions.insert({transactionShortHash, newTransaction});
      }

      EventId timeout;
      std::ostringstream stringStream;
      std::string blockHash;
      std::string parentBlockHash;

      stringStream << height << "/" << minerId;
      blockHash = stringStream.str();

      if (m_onlyHeadersReceived.find(blockHash) != m_onlyHeadersReceived.end())
        m_onlyHeadersReceived.erase(blockHash);

      stringStream.clear();
      stringStream.str("");

      stringStream << parentHeight << "/" << parentMinerId;
      parentBlockHash = stringStream.str();

      if (!m_blockchain.HasBlock(parentHeight, parentMinerId) && !m_blockchain.IsOrphan(parentHeight, parentMinerId) && !ReceivedButNotValidated(parentBlockHash) && !OnlyHeadersReceived(parentBlockHash))
      {
        NS_LOG_INFO("The Block with height = " << d["blocks"][j]["height"].GetInt()
                                               << " and minerId = " << d["blocks"][j]["minerId"].GetInt()
                                               << " is an orphan, so it will be discarded\n");

        m_queueInv.erase(blockHash);
        Simulator::Cancel(m_invTimeouts[blockHash]);
        m_invTimeouts.erase(blockHash);
      }
      else
      {
        Block newBlock(d["blocks"][j]["height"].GetInt(), d["blocks"][j]["minerId"].GetInt(), d["blocks"][j]["parentBlockMinerId"].GetInt(),
                       d["blocks"][j]["size"].GetInt(), d["blocks"][j]["timeCreated"].GetDouble(),
                       Simulator::Now().GetSeconds(), transactionCount, blockTransactions, InetSocketAddress::ConvertFrom(from).GetIpv4());

        ReceiveBlock(newBlock);
      }
    }
  }

  void BitcoinNode::ReceiveBlock(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("ReceiveBlock: At time " << Simulator::Now().GetSeconds()
                                         << "s bitcoin node " << GetNode()->GetId() << " received " << newBlock);

    std::ostringstream stringStream;
    std::string blockHash = stringStream.str();

    stringStream << newBlock.GetBlockHeight() << "/" << newBlock.GetMinerId();
    blockHash = stringStream.str();

    if (m_blockchain.HasBlock(newBlock) || m_blockchain.IsOrphan(newBlock) || ReceivedButNotValidated(blockHash))
    {
      NS_LOG_INFO("ReceiveBlock: Bitcoin node " << GetNode()->GetId() << " has already added this block in the m_blockchain: " << newBlock);

      if (m_invTimeouts.find(blockHash) != m_invTimeouts.end())
      {
        m_queueInv.erase(blockHash);
        Simulator::Cancel(m_invTimeouts[blockHash]);
        m_invTimeouts.erase(blockHash);
      }
    }
    else
    {
      NS_LOG_INFO("ReceiveBlock: Bitcoin node " << GetNode()->GetId() << " has NOT added this block in the m_blockchain: " << newBlock);

      m_receivedNotValidated[blockHash] = newBlock;
      //PrintQueueInv();
      //PrintInvTimeouts();

      if (m_invTimeouts.find(blockHash) != m_invTimeouts.end())
      {
        m_queueInv.erase(blockHash);
        Simulator::Cancel(m_invTimeouts[blockHash]);
        m_invTimeouts.erase(blockHash);
      }

      //PrintQueueInv();
      //PrintInvTimeouts();
      ValidateBlock(newBlock);
    }
  }

  void BitcoinNode::SendInv(std::string packetInfo, Ptr<Socket> to)
  {
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("SendBlock: At time " << Simulator::Now().GetSeconds()
                                      << "s bitcoin miner " << GetNode()->GetId() << " send "
                                      << packetInfo << " to " << to);

    rapidjson::Document d;

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    d.Parse(packetInfo.c_str());
    d.Accept(writer);

    SendMessage_1(NO_MESSAGE, INV, d, to);
  }

  void BitcoinNode::SendInvTxn(std::string packet, Address &fromAddress)
  {
    // Send the InvTxn to all the peers - Lihao
    // NS_LOG_FUNCTION(this);
    //std::cout << "Enter SendInvTxn function" << std::endl;

    double sendTime;
    double eventTime;
    Ipv4Address outgoingIpv4Address;

    rapidjson::Document d;
    d.Parse(packet.c_str());
    double invTxnMessageSize = m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;

    sendTime = invTxnMessageSize / m_uploadSpeed;
    Ipv4Address fromIpv4Address = InetSocketAddress::ConvertFrom(fromAddress).GetIpv4();

    for (std::vector<Ipv4Address>::const_iterator i = m_peersAddresses.begin(); i != m_peersAddresses.end(); ++i)
    {
      outgoingIpv4Address = *i;
      if (outgoingIpv4Address == fromIpv4Address)
      {
        //Fix: Don't send back the transaction to where it come from --Lihao
        continue;
      }
      eventTime = ManageSendTime(sendTime);
      Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_4, this, NO_MESSAGE, INV_TXN, packet, outgoingIpv4Address);
    }
  }

  void BitcoinNode::SendGetData(std::string packetInfo, double messageSize, Address &from)
  {

    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);
    Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, INV, GET_DATA, packetInfo, from);
  }

  void BitcoinNode::SendBlock(std::string packetInfo, double messageSize, Address &from)
  {

    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);

    if (m_protocolType == COMPACT)
      Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, GET_DATA, COMPACT_BLOCK, packetInfo, from);
    else
      Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, GET_DATA, BLOCK, packetInfo, from);
  }

  void BitcoinNode::SendGetDataTxn(std::string packetInfo, double messageSize, Address &from)
  {

    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);
    Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, INV_TXN, GET_DATA_TXN, packetInfo, from);
  }

  void BitcoinNode::SendTransaction(std::string packetInfo, double messageSize, Address &from)
  {
    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);
    Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, GET_DATA, TXN, packetInfo, from);
  }

  void BitcoinNode::SendGetBlockTransactions(std::string packetInfo, double messageSize, Address &from)
  {
    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);
    Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, COMPACT_BLOCK, GET_BLOCK_TXNS, packetInfo, from);
  }

  void BitcoinNode::SendBlockTransactions(std::string packetInfo, double messageSize, Address &from)
  {
    double sendTime = messageSize / m_uploadSpeed;
    double eventTime;

    eventTime = ManageSendTime(sendTime);
    Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_3, this, GET_BLOCK_TXNS, BLOCK_TXNS, packetInfo, from);
  }

  void BitcoinNode::ReceivedHigherBlock(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);
    NS_LOG_INFO("ReceivedHigherBlock: Bitcoin node " << GetNode()->GetId() << " added a new block in the m_blockchain with higher height: " << newBlock);
  }

  void BitcoinNode::ValidateBlock(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);

    const Block *parent = m_blockchain.GetParent(newBlock);

    if (parent == nullptr)
    {
      NS_LOG_INFO("ValidateBlock: Block " << newBlock << " is an orphan\n");

      m_blockchain.AddOrphan(newBlock);
      //m_blockchain.PrintOrphans();
    }
    else
    {
      NS_LOG_INFO("ValidateBlock: Block's " << newBlock << " parent is " << *parent << "\n");

      /**
     * Block is not orphan, so we can go on validating
     */

      const int averageBlockSizeBytes = 458263;
      const double averageValidationTimeSeconds = 0.174;
      double validationTime = averageValidationTimeSeconds * newBlock.GetBlockSizeBytes() / averageBlockSizeBytes;

      Simulator::Schedule(Seconds(validationTime), &BitcoinNode::AfterBlockValidation, this, newBlock);
      NS_LOG_INFO("ValidateBlock: The Block " << newBlock << " will be validated in "
                                              << validationTime << "s");
    }
  }

  void BitcoinNode::AfterBlockValidation(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);

    int height = newBlock.GetBlockHeight();
    int minerId = newBlock.GetMinerId();
    std::ostringstream stringStream;
    std::string blockHash = stringStream.str();

    stringStream << height << "/" << minerId;
    blockHash = stringStream.str();

    RemoveReceivedButNotValidated(blockHash);

    NS_LOG_INFO("AfterBlockValidation: At time " << Simulator::Now().GetSeconds()
                                                 << "s bitcoin node " << GetNode()->GetId()
                                                 << " validated block " << newBlock);

    if (newBlock.GetBlockHeight() > m_blockchain.GetBlockchainHeight())
      ReceivedHigherBlock(newBlock);

    if (m_blockchain.IsOrphan(newBlock))
    {
      NS_LOG_INFO("AfterBlockValidation: Block " << newBlock << " was orphan");
      m_blockchain.RemoveOrphan(newBlock);
    }

    /**
   * Add Block in the blockchain.
   * Update m_meanBlockReceiveTime with the timeReceived of the newly received block.
   */

    m_meanBlockReceiveTime = (m_blockchain.GetTotalBlocks() - 1) / static_cast<double>(m_blockchain.GetTotalBlocks()) * m_meanBlockReceiveTime + (newBlock.GetTimeReceived() - m_previousBlockReceiveTime) / (m_blockchain.GetTotalBlocks());
    m_previousBlockReceiveTime = newBlock.GetTimeReceived();

    m_meanBlockPropagationTime = (m_blockchain.GetTotalBlocks() - 1) / static_cast<double>(m_blockchain.GetTotalBlocks()) * m_meanBlockPropagationTime + (newBlock.GetTimeReceived() - newBlock.GetTimeCreated()) / (m_blockchain.GetTotalBlocks());

    m_meanBlockSize = (m_blockchain.GetTotalBlocks() - 1) / static_cast<double>(m_blockchain.GetTotalBlocks()) * m_meanBlockSize + (newBlock.GetBlockSizeBytes()) / static_cast<double>(m_blockchain.GetTotalBlocks());

    m_blockchain.AddBlock(newBlock);

    AdvertiseNewBlock(newBlock);
    ValidateOrphanChildren(newBlock);
  }

  void BitcoinNode::ValidateOrphanChildren(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);

    std::vector<const Block *> children = m_blockchain.GetOrphanChildrenPointers(newBlock);

    if (children.size() == 0)
    {
      NS_LOG_INFO("ValidateOrphanChildren: Block " << newBlock << " has no orphan children\n");
    }
    else
    {
      std::vector<const Block *>::iterator block_it;
      NS_LOG_INFO("ValidateOrphanChildren: Block " << newBlock << " has orphan children:");

      for (block_it = children.begin(); block_it < children.end(); block_it++)
      {
        NS_LOG_INFO("\t" << **block_it);
        ValidateBlock(**block_it);
      }
    }
  }

  void BitcoinNode::AdvertiseNewBlock(const Block &newBlock)
  {
    NS_LOG_FUNCTION(this);

    rapidjson::Document d;
    rapidjson::Value value;
    rapidjson::Value array(rapidjson::kArrayType);
    std::ostringstream stringStream;
    std::string blockHash = stringStream.str();
    d.SetObject();

    value.SetString("block");
    d.AddMember("type", value, d.GetAllocator());

    if (m_protocolType == STANDARD_PROTOCOL || m_protocolType == COMPACT)
    {
      value = INV;
      d.AddMember("message", value, d.GetAllocator());

      stringStream << newBlock.GetBlockHeight() << "/" << newBlock.GetMinerId();
      blockHash = stringStream.str();
      value.SetString(blockHash.c_str(), blockHash.size(), d.GetAllocator());
      array.PushBack(value, d.GetAllocator());
      d.AddMember("inv", array, d.GetAllocator());
    }
    else if (m_protocolType == SENDHEADERS)
    {
      rapidjson::Value blockInfo(rapidjson::kObjectType);

      value = HEADERS;
      d.AddMember("message", value, d.GetAllocator());

      value = newBlock.GetBlockHeight();
      blockInfo.AddMember("height", value, d.GetAllocator());

      value = newBlock.GetMinerId();
      blockInfo.AddMember("minerId", value, d.GetAllocator());

      value = newBlock.GetParentBlockMinerId();
      blockInfo.AddMember("parentBlockMinerId", value, d.GetAllocator());

      value = newBlock.GetBlockSizeBytes();
      blockInfo.AddMember("size", value, d.GetAllocator());

      value = newBlock.GetTimeCreated();
      blockInfo.AddMember("timeCreated", value, d.GetAllocator());

      value = newBlock.GetTimeReceived();
      blockInfo.AddMember("timeReceived", value, d.GetAllocator());

      array.PushBack(blockInfo, d.GetAllocator());
      d.AddMember("blocks", array, d.GetAllocator());
    }

    // Stringify the DOM
    rapidjson::StringBuffer packetInfo;
    rapidjson::Writer<rapidjson::StringBuffer> writer(packetInfo);
    d.Accept(writer);

    for (std::vector<Ipv4Address>::const_iterator i = m_peersAddresses.begin(); i != m_peersAddresses.end(); ++i)
    {
      if (*i != newBlock.GetReceivedFromIpv4())
      {
        if (m_protocolType == STANDARD_PROTOCOL || m_protocolType == COMPACT)
        {
          double eventTime;
          double sendTime;
          double invMessageSize = m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes;

          sendTime = invMessageSize / m_uploadSpeed;
          std::string packet = packetInfo.GetString();
          eventTime = ManageSendTime(sendTime);
          Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendInv, this, packet, m_peersSockets[*i]);
        }
        //else if (m_protocolType == SENDHEADERS)
        //  m_nodeStats->headersSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;
        NS_LOG_INFO("AdvertiseNewBlock: At time " << Simulator::Now().GetSeconds()
                                                  << "s bitcoin node " << GetNode()->GetId() << " advertised a new Block: "
                                                  << newBlock << " to " << *i);
      }
    }
    Simulator::Schedule(Seconds(50), &BitcoinNode::DeleteBlockTransactionsFromMempool, this, newBlock);
  }

  void BitcoinNode::GenerateTransactions(void)
  {
    // NS_LOG_FUNCTION(this);

    m_fixedTransactionTimeGeneration = 1;

    if (m_fixedTransactionTimeGeneration > 0)
    {
      m_nextTransactionTime = m_fixedTransactionTimeGeneration;

      m_nextTransactionGenerationEvent = Simulator::Schedule(Seconds(m_fixedTransactionTimeGeneration), &BitcoinNode::CreateTransaction, this);
    }
    else
    {
      std::normal_distribution<> d{10, 50};

      m_nextTransactionTime = abs(std::round(d(m_generator)));

      m_nextTransactionGenerationEvent = Simulator::Schedule(Seconds(m_nextTransactionTime), &BitcoinNode::CreateTransaction, this);
    }
  }

  void BitcoinNode::CreateTransaction(void)
  {
    // NS_LOG_FUNCTION(this);
    //std::cout << "Create Transaction: At Time " << Simulator::Now().GetSeconds() << std::endl;

    int i = rand();
    double transactionSizeBytes; //generate the size later

    std::array<double, 8> iSize{300, 400, 600, 1400, 3000, 4000, 6000, 14000};
    std::array<double, 7> wSize{5, 25, 10, 10, 5, 25, 20};
    m_transactionSizeDistribution = std::piecewise_constant_distribution<double>(iSize.begin(), iSize.end(), wSize.begin());
    transactionSizeBytes = m_transactionSizeDistribution(m_generator);

    CryptoPP::SHA256 hash;
    byte digest[CryptoPP::SHA256::DIGESTSIZE];
    std::string message = std::to_string(i);
    hash.CalculateDigest(digest, (byte *)message.c_str(), message.length());

    CryptoPP::HexEncoder encoder;
    std::string transactionHash;
    encoder.Attach(new CryptoPP::StringSink(transactionHash));
    encoder.Put(digest, sizeof(digest));
    encoder.MessageEnd();
    std::string transactionShortHash = transactionHash.substr(0, 8);

    Transaction newTransaction(transactionSizeBytes, transactionHash, transactionShortHash);

    m_mempool.AddTransaction(newTransaction);

    rapidjson::Document transaction;
    transaction.SetObject();

    rapidjson::Value value;
    rapidjson::Value array(rapidjson::kArrayType);

    value.SetString("txn"); //Remove
    transaction.AddMember("type", value, transaction.GetAllocator());

    value = TXN;
    transaction.AddMember("message", value, transaction.GetAllocator());

    value = transactionSizeBytes;
    transaction.AddMember("size", value, transaction.GetAllocator());

    value.SetString(transactionHash.c_str(), transactionHash.size(), transaction.GetAllocator());
    transaction.AddMember("hash", value, transaction.GetAllocator());

    value.SetString(transactionShortHash.c_str(), transactionShortHash.size(), transaction.GetAllocator());
    transaction.AddMember("shortHash", value, transaction.GetAllocator());

    BroadcastTransaction(transaction);

    double currentTime = Simulator::Now().GetSeconds();
    if (currentTime <= 1200) //After 10 min, we stop generate transaction! (Just for convenience) - Lihao
    {
      GenerateTransactions();
    }
  }

  void BitcoinNode::BroadcastTransaction(rapidjson::Document &d)
  {
    //std::cout << "Broadcast Transaction: At Time " << Simulator::Now().GetSeconds() << std::endl;

    // 2019-2-28 Fix: Add transaction into sending queue -Lihao
    // NS_LOG_FUNCTION(this);
    double sendTime;
    double eventTime;
    Ipv4Address outgoingIpv4Address;

    rapidjson::StringBuffer transaction;
    rapidjson::Writer<rapidjson::StringBuffer> transactionWriter(transaction);
    d.Accept(transactionWriter);
    std::string packet = transaction.GetString();

    double transactionSizeBytes = d["size"].GetDouble();

    sendTime = transactionSizeBytes / m_uploadSpeed;
    for (std::vector<Ipv4Address>::const_iterator i = m_peersAddresses.begin(); i != m_peersAddresses.end(); ++i)
    {
      outgoingIpv4Address = *i;
      eventTime = ManageSendTime(sendTime);
      Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendMessage_4, this, NO_MESSAGE, TXN, packet, outgoingIpv4Address);
    }
  }

  void BitcoinNode::DeleteBlockTransactionsFromMempool(const Block &newBlock)
  {
    std::map<std::string, Transaction> thisBlockTransactions = newBlock.GetBlockTransactions();
    for (std::map<std::string, Transaction>::const_iterator it = thisBlockTransactions.begin(); it != thisBlockTransactions.end(); it++)
    {
      std::string shortHash = (it->second).GetTransactionShortHash();
      m_mempool.DeleteTransactionWithShortHash(shortHash);
    }
  }

  void BitcoinNode::SendMessage_1(enum Messages receivedMessage, enum Messages responseMessage, rapidjson::Document & d, Ptr<Socket> outgoingSocket)
  {
    // NS_LOG_FUNCTION(this);
    const uint8_t delimiter[] = "#";

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    d["message"].SetInt(responseMessage);
    d.Accept(writer);
    // NS_LOG_INFO ("Node " << GetNode ()->GetId () << " got a "
    //              << getMessageName(receivedMessage) << " message"
    //              << " and sent a " << getMessageName(responseMessage)
    //              << " message: " << buffer.GetString());

    outgoingSocket->Send(reinterpret_cast<const uint8_t *>(buffer.GetString()), buffer.GetSize(), 0);
    outgoingSocket->Send(delimiter, 1, 0);

    switch (d["message"].GetInt())
    {
    case INV:
    {
      m_nodeStats->invSentBytes += m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes;
      break;
    }
    case INV_TXN:
    {
      m_nodeStats->invTxnSentBytes += m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
      break;
    }
    case GET_HEADERS:
    {
      m_nodeStats->getHeadersSentBytes += m_bitcoinMessageHeader + m_getHeadersSizeBytes;
      break;
    }
    case HEADERS:
    {
      m_nodeStats->headersSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;
      break;
    }
    case BLOCK:
    {
      for (int k = 0; k < d["blocks"].Size(); k++)
        m_nodeStats->blockSentBytes += d["blocks"][k]["size"].GetInt();
      m_nodeStats->blockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_DATA:
    {
      m_nodeStats->getDataSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
      break;
    }
    case GET_DATA_TXN:
    {
      m_nodeStats->getDataTxnSentBytes += m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
      break;
    }
    case COMPACT_BLOCK:
    {
      //Fix : For compact block, we only calculate the header size.
      for (int k = 0; k < d["blocks"].Size(); k++)
      {
        int transactionCount = d["blocks"][k]["transactionCount"].GetInt();
        m_nodeStats->compactBlockSentBytes += (transactionCount * 6) + 8 + 1 + 250;
      }
      m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_BLOCK_TXNS:
    {
      int missingTransactionCount = d["missingTransactions"].Size();
      double blockTransactionsMissingRequest = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length
      blockTransactionsMissingRequest += m_bitcoinMessageHeader;
      m_nodeStats->getblocktxnSentBytes += m_bitcoinMessageHeader + blockTransactionsMissingRequest;
      break;
    }
    case BLOCK_TXNS:
    {
      double missingTransactionSize = 0;
      for (int i = 0; i < d["missingTransactions"].Size(); i++)
      {
        double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
        missingTransactionSize += missingTxSize;
      }
      missingTransactionSize += m_bitcoinMessageHeader;
      m_nodeStats->blocktxnSentBytes += m_bitcoinMessageHeader + missingTransactionSize;
      break;
    }
    case TXN:
    {
      double transactionSizeBytes = d["size"].GetDouble();
      m_nodeStats->transactionSentBytes += m_bitcoinMessageHeader + transactionSizeBytes;
      break;
    }
    }
  }

  void BitcoinNode::SendMessage_2(enum Messages receivedMessage, enum Messages responseMessage, rapidjson::Document & d, Address & outgoingAddress)
  {
    NS_LOG_FUNCTION(this);

    const uint8_t delimiter[] = "#";

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    d["message"].SetInt(responseMessage);
    d.Accept(writer);
    NS_LOG_INFO("Node " << GetNode()->GetId() << " got a "
                        << getMessageName(receivedMessage) << " message"
                        << " and sent a " << getMessageName(responseMessage)
                        << " message: " << buffer.GetString());

    Ipv4Address outgoingIpv4Address = InetSocketAddress::ConvertFrom(outgoingAddress).GetIpv4();
    std::map<Ipv4Address, Ptr<Socket>>::iterator it = m_peersSockets.find(outgoingIpv4Address);

    if (it == m_peersSockets.end()) //Create the socket if it doesn't exist
    {
      m_peersSockets[outgoingIpv4Address] = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
      m_peersSockets[outgoingIpv4Address]->Connect(InetSocketAddress(outgoingIpv4Address, m_bitcoinPort));
    }

    m_peersSockets[outgoingIpv4Address]->Send(reinterpret_cast<const uint8_t *>(buffer.GetString()), buffer.GetSize(), 0);
    m_peersSockets[outgoingIpv4Address]->Send(delimiter, 1, 0);

    switch (d["message"].GetInt())
    {
    case INV:
    {
      m_nodeStats->invSentBytes += m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes;
      break;
    }
    case INV_TXN:
    {
      m_nodeStats->invTxnSentBytes += m_bitcoinMessageHeader + m_countBytes +  m_inventorySizeBytes;
      break;
    }
    case GET_HEADERS:
    {
      m_nodeStats->getHeadersSentBytes += m_bitcoinMessageHeader + m_getHeadersSizeBytes;
      break;
    }
    case HEADERS:
    {
      m_nodeStats->headersSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;
      break;
    }
    case BLOCK:
    {
      for (int k = 0; k < d["blocks"].Size(); k++)
        m_nodeStats->blockSentBytes += d["blocks"][k]["size"].GetInt();
      m_nodeStats->blockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_DATA:
    {
      m_nodeStats->getDataSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
      break;
    }
    case GET_DATA_TXN:
    {
      m_nodeStats->getDataTxnSentBytes += m_bitcoinMessageHeader + m_countBytes +  m_inventorySizeBytes;
      break;
    }
    case COMPACT_BLOCK:
    {
      //Fix : For compact block, we only calculate the header size.
      for (int k = 0; k < d["blocks"].Size(); k++)
      {
        int transactionCount = d["blocks"][k]["transactionCount"].GetInt();
        m_nodeStats->compactBlockSentBytes += (transactionCount * 6) + 8 + 1 + 250;
      }
      m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;

      //for (int k = 0; k < d["blocks"].Size(); k++)
      //  m_nodeStats->compactBlockSentBytes += d["blocks"][k]["size"].GetInt();
      //m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_BLOCK_TXNS:
    {
      int missingTransactionCount = d["missingTransactions"].Size();
      double blockTransactionsMissingRequest = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length
      m_nodeStats->getblocktxnSentBytes += m_bitcoinMessageHeader + blockTransactionsMissingRequest;
      break;
    }
    case BLOCK_TXNS:
    {
      double missingTransactionSize = 0;
      for (int i = 0; i < d["missingTransactions"].Size(); i++)
      {
        double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
        missingTransactionSize += missingTxSize;
      }
      m_nodeStats->blocktxnSentBytes += m_bitcoinMessageHeader + missingTransactionSize;
      break;
    }
    case TXN:
    {
      double transactionSizeBytes = d["size"].GetDouble();
      m_nodeStats->transactionSentBytes += m_bitcoinMessageHeader + transactionSizeBytes;
      break;
    }
    }
  }

  void BitcoinNode::SendMessage_3(enum Messages receivedMessage, enum Messages responseMessage, std::string packet, Address & outgoingAddress)
  {
    NS_LOG_FUNCTION(this);

    const uint8_t delimiter[] = "#";
    rapidjson::Document d;

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    d.Parse(packet.c_str());
    d["message"].SetInt(responseMessage);
    d.Accept(writer);
    NS_LOG_INFO("Node " << GetNode()->GetId() << " got a "
                        << getMessageName(receivedMessage) << " message"
                        << " and sent a " << getMessageName(responseMessage)
                        << " message: " << buffer.GetString());

    Ipv4Address outgoingIpv4Address = InetSocketAddress::ConvertFrom(outgoingAddress).GetIpv4();
    std::map<Ipv4Address, Ptr<Socket>>::iterator it = m_peersSockets.find(outgoingIpv4Address);

    if (it == m_peersSockets.end()) //Create the socket if it doesn't exist
    {
      m_peersSockets[outgoingIpv4Address] = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
      m_peersSockets[outgoingIpv4Address]->Connect(InetSocketAddress(outgoingIpv4Address, m_bitcoinPort));
    }

    //std::cout << buffer.GetString() << std::endl;

    m_peersSockets[outgoingIpv4Address]->Send(reinterpret_cast<const uint8_t *>(buffer.GetString()), buffer.GetSize(), 0);
    m_peersSockets[outgoingIpv4Address]->Send(delimiter, 1, 0);

    switch (d["message"].GetInt())
    {
    case INV:
    {
      m_nodeStats->invSentBytes += m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes;
      break;
    }
    case INV_TXN:
    {
      m_nodeStats->invTxnSentBytes += m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
      break;
    }
    case GET_HEADERS:
    {
      m_nodeStats->getHeadersSentBytes += m_bitcoinMessageHeader + m_getHeadersSizeBytes;
      break;
    }
    case HEADERS:
    {
      m_nodeStats->headersSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;
      break;
    }
    case BLOCK:
    {
      for (int k = 0; k < d["blocks"].Size(); k++)
        m_nodeStats->blockSentBytes += d["blocks"][k]["size"].GetInt();
      m_nodeStats->blockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_DATA:
    {
      m_nodeStats->getDataSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
      break;
    }
    case GET_DATA_TXN:
    {
      m_nodeStats->getDataTxnSentBytes += m_bitcoinMessageHeader + m_countBytes + m_inventorySizeBytes;
      break;
    }
    case COMPACT_BLOCK:
    {
      //Fix : For compact block, we only calculate the header size.
      for (int k = 0; k < d["blocks"].Size(); k++)
      {
        int transactionCount = d["blocks"][k]["transactionCount"].GetInt();
        m_nodeStats->compactBlockSentBytes += (transactionCount * 6) + 8 + 1 + 250;
      }
      m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;

      //for (int k = 0; k < d["blocks"].Size(); k++)
      //  m_nodeStats->compactBlockSentBytes += d["blocks"][k]["size"].GetInt();
      //m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_BLOCK_TXNS:
    {
      int missingTransactionCount = d["missingTransactions"].Size();
      double blockTransactionsMissingRequest = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length
      m_nodeStats->getblocktxnSentBytes += m_bitcoinMessageHeader + blockTransactionsMissingRequest;
      break;
    }
    case BLOCK_TXNS:
    {
      double missingTransactionSize = 0;
      for (int i = 0; i < d["missingTransactions"].Size(); i++)
      {
        double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
        missingTransactionSize += missingTxSize;
      }
      m_nodeStats->blocktxnSentBytes += m_bitcoinMessageHeader + missingTransactionSize;
      break;
    }
    case TXN:
    {
      double transactionSizeBytes = d["size"].GetDouble();
      m_nodeStats->transactionSentBytes += m_bitcoinMessageHeader + transactionSizeBytes;
      break;
    }
    }
  }

  void BitcoinNode::SendMessage_4(enum Messages receivedMessage, enum Messages responseMessage, std::string packet, Ipv4Address outgoingIpv4Address)
  {
    NS_LOG_FUNCTION(this);

    const uint8_t delimiter[] = "#";
    rapidjson::Document d;

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    d.Parse(packet.c_str());
    d["message"].SetInt(responseMessage);
    d.Accept(writer);
    NS_LOG_INFO("Node " << GetNode()->GetId() << " got a "
                        << getMessageName(receivedMessage) << " message"
                        << " and sent a " << getMessageName(responseMessage)
                        << " message: " << buffer.GetString());

    std::map<Ipv4Address, Ptr<Socket>>::iterator it = m_peersSockets.find(outgoingIpv4Address);

    if (it == m_peersSockets.end()) //Create the socket if it doesn't exist
    {
      m_peersSockets[outgoingIpv4Address] = Socket::CreateSocket(GetNode(), TcpSocketFactory::GetTypeId());
      m_peersSockets[outgoingIpv4Address]->Connect(InetSocketAddress(outgoingIpv4Address, m_bitcoinPort));
    }

    //std::cout << buffer.GetString() << std::endl;

    m_peersSockets[outgoingIpv4Address]->Send(reinterpret_cast<const uint8_t *>(buffer.GetString()), buffer.GetSize(), 0);
    m_peersSockets[outgoingIpv4Address]->Send(delimiter, 1, 0);

    switch (d["message"].GetInt())
    {
    case INV:
    {
      m_nodeStats->invSentBytes += m_bitcoinMessageHeader + m_countBytes + d["inv"].Size() * m_inventorySizeBytes;
      break;
    }
    case INV_TXN:
    {
      m_nodeStats->invTxnSentBytes += m_bitcoinMessageHeader + m_countBytes +  m_inventorySizeBytes;
      break;
    }
    case GET_HEADERS:
    {
      m_nodeStats->getHeadersSentBytes += m_bitcoinMessageHeader + m_getHeadersSizeBytes;
      break;
    }
    case HEADERS:
    {
      m_nodeStats->headersSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_headersSizeBytes;
      break;
    }
    case BLOCK:
    {
      for (int k = 0; k < d["blocks"].Size(); k++)
        m_nodeStats->blockSentBytes += d["blocks"][k]["size"].GetInt();
      m_nodeStats->blockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_DATA:
    {
      m_nodeStats->getDataSentBytes += m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;
      break;
    }
    case GET_DATA_TXN:
    {
      m_nodeStats->getDataTxnSentBytes += m_bitcoinMessageHeader + m_countBytes +  m_inventorySizeBytes;
      break;
    }
    case COMPACT_BLOCK:
    {
      //Fix : For compact block, we only calculate the header size.
      for (int k = 0; k < d["blocks"].Size(); k++)
      {
        int transactionCount = d["blocks"][k]["transactionCount"].GetInt();
        m_nodeStats->compactBlockSentBytes += (transactionCount * 6) + 8 + 1 + 250;
      }
      m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;

      //for (int k = 0; k < d["blocks"].Size(); k++)
      //  m_nodeStats->compactBlockSentBytes += d["blocks"][k]["size"].GetInt();
      //m_nodeStats->compactBlockSentBytes += m_bitcoinMessageHeader;
      break;
    }
    case GET_BLOCK_TXNS:
    {
      int missingTransactionCount = d["missingTransactions"].Size();
      double blockTransactionsMissingRequest = 32 + 3 + (missingTransactionCount * 3); //blockHash + index_length
      m_nodeStats->getblocktxnSentBytes += m_bitcoinMessageHeader + blockTransactionsMissingRequest;
      break;
    }
    case BLOCK_TXNS:
    {
      double missingTransactionSize = 0;
      for (int i = 0; i < d["missingTransactions"].Size(); i++)
      {
        double missingTxSize = d["missingTransactions"][i]["transactionSizeBytes"].GetDouble();
        missingTransactionSize += missingTxSize;
      }
      m_nodeStats->blocktxnSentBytes += m_bitcoinMessageHeader + missingTransactionSize;
      break;
    }
    case TXN:
    {
      double transactionSizeBytes = d["size"].GetDouble();
      m_nodeStats->transactionSentBytes += m_bitcoinMessageHeader + transactionSizeBytes;
      break;
    }
    }
  }

  void BitcoinNode::PrintQueueInv()
  {
    NS_LOG_FUNCTION(this);

    std::cout << "Node " << GetNode()->GetId() << ": The queueINV is:\n";

    for (auto &elem : m_queueInv)
    {
      std::vector<Address>::iterator block_it;
      std::cout << "  " << elem.first << ":";

      for (block_it = elem.second.begin(); block_it < elem.second.end(); block_it++)
      {
        std::cout << " " << InetSocketAddress::ConvertFrom(*block_it).GetIpv4();
      }
      std::cout << "\n";
    }
    std::cout << std::endl;
  }

  void BitcoinNode::PrintInvTimeouts()
  {
    NS_LOG_FUNCTION(this);

    std::cout << "Node " << GetNode()->GetId() << ": The m_invTimeouts is:\n";

    for (auto &elem : m_invTimeouts)
    {
      std::cout << "  " << elem.first << ":\n";
    }
    std::cout << std::endl;
  }

  void BitcoinNode::PrintOnlyHeadersReceived()
  {
    NS_LOG_FUNCTION(this);

    std::cout << "Node " << GetNode()->GetId() << ": The m_onlyHeadersReceived is:\n";

    for (auto &elem : m_onlyHeadersReceived)
    {
      std::vector<Address>::iterator block_it;
      std::cout << "  " << elem.first << ": " << elem.second << "\n";
    }
    std::cout << std::endl;
  }

  void BitcoinNode::InvTimeoutExpired(std::string blockHash)
  {
    NS_LOG_FUNCTION(this);

    std::string invDelimiter = "/";
    size_t invPos = blockHash.find(invDelimiter);

    int height = atoi(blockHash.substr(0, invPos).c_str());
    int minerId = atoi(blockHash.substr(invPos + 1, blockHash.size()).c_str());

    NS_LOG_INFO("Node " << GetNode()->GetId() << ": At time " << Simulator::Now().GetSeconds()
                        << " the timeout for block " << blockHash << " expired");

    m_nodeStats->blockTimeouts++;
    //PrintQueueInv();
    //PrintInvTimeouts();

    m_queueInv[blockHash].erase(m_queueInv[blockHash].begin());
    m_invTimeouts.erase(blockHash);

    //PrintQueueInv();
    //PrintInvTimeouts();

    if (!m_queueInv[blockHash].empty() && !m_blockchain.HasBlock(height, minerId) && !m_blockchain.IsOrphan(height, minerId) && !ReceivedButNotValidated(blockHash))
    {

      double receiveTime;
      double eventTime;

      rapidjson::Document d;
      EventId timeout;
      rapidjson::Value value(INV);
      rapidjson::Value array(rapidjson::kArrayType);

      d.SetObject();

      d.AddMember("message", value, d.GetAllocator());

      value.SetString("block");
      d.AddMember("type", value, d.GetAllocator());

      value.SetString(blockHash.c_str(), blockHash.size(), d.GetAllocator());
      array.PushBack(value, d.GetAllocator());
      d.AddMember("blocks", array, d.GetAllocator());
      
      int index = rand() % m_queueInv[blockHash].size();
      Address temp = m_queueInv[blockHash][0];
      m_queueInv[blockHash][0] = m_queueInv[blockHash][index];
      m_queueInv[blockHash][index] = temp;

      rapidjson::StringBuffer inv;
      rapidjson::Writer<rapidjson::StringBuffer> invWriter(inv);
      d.Accept(invWriter);
      std::string help = inv.GetString();

      receiveTime = 0;
      eventTime = ManageReceiveTime(receiveTime, receiveTime);
      double getdataMessageSize = m_bitcoinMessageHeader + m_countBytes + d["blocks"].Size() * m_inventorySizeBytes;

      Simulator::Schedule(Seconds(eventTime), &BitcoinNode::SendGetData, this, help, getdataMessageSize, temp);
      //SendMessage_2(INV, GET_HEADERS, d, *(m_queueInv[blockHash].begin()));
      //SendMessage_2(INV, GET_DATA, d, *(m_queueInv[blockHash].begin()));

      timeout = Simulator::Schedule(m_invTimeoutMinutes, &BitcoinNode::InvTimeoutExpired, this, blockHash);
      m_invTimeouts[blockHash] = timeout;
    }
    else
      m_queueInv.erase(blockHash);

    //PrintQueueInv();
    //PrintInvTimeouts();
  }

  bool BitcoinNode::ReceivedButNotValidated(std::string blockHash)
  {
    NS_LOG_FUNCTION(this);

    if (m_receivedNotValidated.find(blockHash) != m_receivedNotValidated.end())
      return true;
    else
      return false;
  }

  void BitcoinNode::RemoveReceivedButNotValidated(std::string blockHash)
  {
    NS_LOG_FUNCTION(this);

    if (m_receivedNotValidated.find(blockHash) != m_receivedNotValidated.end())
    {
      m_receivedNotValidated.erase(blockHash);
    }
    else
    {
      NS_LOG_WARN(blockHash << " was not found in m_receivedNotValidated");
    }
  }

  bool BitcoinNode::OnlyHeadersReceived(std::string blockHash)
  {
    NS_LOG_FUNCTION(this);

    if (m_onlyHeadersReceived.find(blockHash) != m_onlyHeadersReceived.end())
      return true;
    else
      return false;
  }

  double BitcoinNode::ManageSendTime(double sendTime)
  {
    double eventTime;

    //Lock for m_sendQueueTimes -Lihao
    TX_mtx.lock();
    std::lock_guard<std::mutex> lck(TX_mtx, std::adopt_lock);

    if (m_sendQueueTimes.size() == 0 || Simulator::Now().GetSeconds() > m_sendQueueTimes.back())
    {
      eventTime = 0;
      m_sendQueueTimes.push_back(Simulator::Now().GetSeconds() + eventTime + sendTime);
    }
    else
    {
      eventTime = m_sendQueueTimes.back() - Simulator::Now().GetSeconds();
      m_sendQueueTimes.push_back(Simulator::Now().GetSeconds() + eventTime + sendTime);
      // After we push back a timestamp into the time queue, delete the first on --Lihao
      //RemoveSendTime();
      m_sendQueueTimes.erase(m_sendQueueTimes.begin());
    }
    return eventTime;
  }

  void BitcoinNode::RemoveSendTime()
  {
    NS_LOG_FUNCTION(this);

    NS_LOG_INFO("RemoveSendTime: At Time " << Simulator::Now().GetSeconds() << " " << m_sendQueueTimes.front() << " was removed");
    m_sendQueueTimes.erase(m_sendQueueTimes.begin());
  }

  double BitcoinNode::ManageReceiveTime(double receiveTime, double eventTime)
  {
    //Lock for m_receiveQueueTimes -Lihao
    RX_mtx.lock();
    std::lock_guard<std::mutex> lck(RX_mtx, std::adopt_lock);

    if (m_receiveQueueTimes.size() == 0 || Simulator::Now().GetSeconds() > m_receiveQueueTimes.back())
    {
      eventTime = receiveTime;
      m_receiveQueueTimes.push_back(Simulator::Now().GetSeconds() + receiveTime);
    }
    else
    {
      // Our Receive BLock Queue is empty. The event_time depends on minSpeed -lihao
      receiveTime = receiveTime + m_receiveQueueTimes.back() - Simulator::Now().GetSeconds();
      eventTime = eventTime + m_receiveQueueTimes.back() - Simulator::Now().GetSeconds();
      m_receiveQueueTimes.push_back(Simulator::Now().GetSeconds() + receiveTime);

      //RemoveReceiveTime();
      m_receiveQueueTimes.erase(m_receiveQueueTimes.begin());
    }
    return eventTime;
  }

  void BitcoinNode::RemoveReceiveTime()
  {
    NS_LOG_FUNCTION(this);

    NS_LOG_INFO("RemoveReceiveTime: At Time " << Simulator::Now().GetSeconds() << " " << m_receiveQueueTimes.front() << " was removed");
    m_receiveQueueTimes.erase(m_receiveQueueTimes.begin());
  }

  void BitcoinNode::HandlePeerClose(Ptr<Socket> socket)
  {
    NS_LOG_FUNCTION(this << socket);
  }

  void BitcoinNode::HandlePeerError(Ptr<Socket> socket)
  {
    NS_LOG_FUNCTION(this << socket);
  }

  void BitcoinNode::HandleAccept(Ptr<Socket> s, const Address &from)
  {
    NS_LOG_FUNCTION(this << s << from);
    s->SetRecvCallback(MakeCallback(&BitcoinNode::HandleRead, this));
  }

} // Namespace ns3
