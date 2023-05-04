TORCH_HOME=/scratch/kumpera/pytorch


uvstore: uvstore.cc
	clang++ -lm -lpthread -lc10 -ltorch -ltorch_cpu -L${TORCH_HOME}/torch/lib -I${TORCH_HOME}/torch/include -Ithird_party/libuv/include  uvstore.cc -o uvstore 

tst: tst.cc
	clang++ -ggdb -lm -lpthread -luv -Ithird_party/libuv/include -Lthird_party/libuv/build/ tst.cc


