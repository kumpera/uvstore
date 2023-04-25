
tst: tst.cc
	clang++ -o tst -Ithird_party/libuv/include -Lthird_party/libuv/build/ -luv  tst.cc
