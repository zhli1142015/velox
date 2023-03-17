# set up WD
sudo mkdir /var/git  
sudo chmod +777 -R /var/git  
sudo chmod +777 -R /usr/local  


# apt install
sudo add-apt-repository ppa:team-xbmc/ppa -y  
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null  
sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main' -y  
sudo add-apt-repository ppa:ubuntu-toolchain-r/test -y  

sudo apt-get update -y  
sudo apt update -y  
sudo apt-get install openjdk-8-jdk -y  
sudo apt install -y cmake ccache ninja-build checkinstall git libssl-dev libdouble-conversion-dev libgoogle-glog-dev libbz2-dev libgflags-dev libgtest-dev libgmock-dev libevent-dev liblz4-dev libre2-dev libsnappy-dev liblzo2-dev bison flex tzdata wget  
sudo apt-get install -y build-essential maven llvm-10 clang-10 libdwarf-dev libcurl4-openssl-dev autoconf pkg-config liblzo2-dev libiberty-dev libtool zlib1g-dev  
sudo apt-get -y install zip unzip tar  


# install gcc9
sudo apt-get install -y gcc-9 g++-9  
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 900 --slave /usr/bin/g++ g++ /usr/bin/g++-9  
sudo apt install -y libiberty-dev libxml2-dev libkrb5-dev libgsasl7-dev libuuid1 uuid-dev  


# install boost
wget -O boost_1_72_0.tar.gz https://sourceforge.net/projects/boost/files/boost/1.72.0/boost_1_72_0.tar.gz/download  
tar xzvf boost_1_72_0.tar.gz  
cd boost_1_72_0/  
./bootstrap.sh --prefix=/usr/  
sudo ./b2 install  


# install zstd
git clone https://github.com/facebook/zstd.git  
cd zstd  
make install  

# install aclocal
wget -O automake-1.16.1.tar.gz http://ftp.gnu.org/gnu/automake/automake-1.16.1.tar.gz  
tar -xzf automake-1.16.1.tar.gz  
cd automake-1.16.1  
./configure  
make  
sudo make install  

