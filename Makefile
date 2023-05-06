TORCH_HOME=/scratch/kumpera/pytorch


uvstore: uvstore.cc uvstore.hpp uvstore-client.cc Makefile
	clang++ -lm -lpthread -lc10 -ltorch -ltorch_cpu -L${TORCH_HOME}/torch/lib -I${TORCH_HOME}/torch/include -I${TORCH_HOME}/third_party/fmt/include -Ithird_party/libuv/include -Wl,-rpath ${TORCH_HOME}/torch/lib uvstore.cc uvstore-client.cc ${TORCH_HOME}/torch/csrc/distributed/c10d/socket.cpp -o uvstore 

tst: tst.cc Makefile
	clang++ -ggdb -lm -lpthread -luv -Ithird_party/libuv/include -Lthird_party/libuv/build/ -Wl,-rpath third_party/libuv/build/ tst.cc -o tst


