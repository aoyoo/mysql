AM_CPPFLAGS=-I${top_srcdir}/src/common \
		-I${top_srcdir}/include \
		-I${top_srcdir}/include/log4cxx	 \
		-I${top_srcdir}/include/libxml \
		-I${top_srcdir}/include/zeromq \
		-I/usr/include/apr-1   \
		-I${top_srcdir}/include/activemq-cpp \
		-I/usr/include/mysql \
		-I${top_srcdir}/src/common/log \
		-I${top_srcdir}/src/common/pubsub \
		-I${top_srcdir}/src/common/db \
		-I${top_srcdir}/src/common/util \
		-I${top_srcdir}/src/common/protobuf 

LDADD= $(top_srcdir)/lib/liblog4cxx.a \
	$(top_srcdir)/lib/libapr-1.a \
	$(top_srcdir)/lib/libaprutil-1.a \
	$(top_srcdir)/src/common/log/liblog.a \
	$(top_srcdir)/src/common/pubsub/libpubsub.a \
	$(top_srcdir)/src/common/util/libutil.a \
	$(top_srcdir)/src/common/protobuf/libpbmsg.a \
	$(top_srcdir)/src/common/db/libStore.a \
	$(top_srcdir)/lib/libxml2.a \
	$(top_srcdir)/lib/libprotobuf.a \
	$(top_srcdir)/lib/libboost_regex.a 


AM_LDFLAGS=-lpthread -lexpat -L/usr/lib64/mysql/ -lmysqlclient 

source_list= Store.h Store.cpp 
test_source_list= test_src/test.cpp

noinst_LIBRARIES=libStore.a
bin_PROGRAMS=test

libStore_a_SOURCES=${source_list}
test_SOURCES=${source_list} ${test_source_list}

