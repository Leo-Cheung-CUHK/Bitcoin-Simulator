# Bitcoin-Simulator, capable of simulating any re-parametrization of Bitcoin
Bitcoin Simulator is built on ns3, the popular discrete-event simulator. We also made use of rapidjson to facilitate the communication process among the nodes. The purpose of this project is to study how consensus parameteres, network characteristics and protocol modifications affect the scalability, security and efficiency of Proof of Work powered blockchains.

Our goal is to make the simulator as realistic as possible. So, we collected real network statistics and incorporated them in the simulator. Specifically, we crawled popular explorers, like blockchain.info to estimate the block generation and block size distribution, and used the bitcoin crawler to find out the average number of nodes in the network and their geographic distribution. Futhermore, we used the data provided by coinscope regarding the connectivity of nodes.

# New Features:
1, Transactions Broadcast and memory pool maintain
2, BIP-152 Compact Block

#Dependencies:
cryptopp/crypto++ Library

# Installation Steps
1, un-comment all the command lines and set the paths correctly in the copy-to-ns3.sh
2, source copy-to-ns3.sh
3, CXXFLAGS="-std=c++11" ./waf configure --build-profile=optimized --out=build/optimized --with-pybindgen=/XX/.../ns-allinone-3.X.X/pybindgen-0.17.0.post58+ngcf00cc0 --enable-mpi --enable-static

4, For Standard Bitcoin Protocol, pls run: ./waf --run bitcoin-test  
   For BIP-152 CompactBlock Protocol, pls run: ./waf --run 'bitcoin-test  --compact=true'
   (if you want to use MPI to accelerate the program, run: mpirun -n <"NoProcessors"> ./waf --run "bitcoin-test")

# Author 
lihaocuhk@gmail.com
