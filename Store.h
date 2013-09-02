#ifndef MCS_Store_H
#define MCS_Store_H

#include <sstream> //for std::stringstream in Store::debugString()
#include <string>
#include <vector>
#include <queue>

#include <boost/noncopyable.hpp>
#include <mysql.h>

using std::string;
using std::vector;
using std::deque;

enum StoreStatusStatus{
	StoreNumERR = -2,
	StoreERR = -1,
	StoreOK = 0,
	StoreToEnd = 1
};

class Store : boost::noncopyable{

public:
	Store(const string host, 
	      const string user, 
				const string passwd, 
				const string dbName, 
				unsigned int port, 
				const string unixSock)
		:host_(host), user_(user), passwd_(passwd), dbName_(dbName), port_(port), unixSock_(unixSock),
		currentBinlogPosition_(-1), hasConnected_(false){
		mysql_init(&mysql_);
	}
	
	int connect(); 
	
	int getFeature(int id, string &feat);
	int getImage(int id, string &image);
	int getImageURL(int id, string &url);
	
	int setFeature(int id, const char* feat, int length);
	int setImage(int id, const char* image, int length);
	int setImageURL(int id, const char* url, int length);
	
	int getBinlogName(deque<string>& result);
	int getBinlogEvents(const string& logName, int position, int limit, vector< vector<string> >& result); 
	
	int getCurrentBinlogName(string& name);
	int getCurrentBinlogEvents(int limit, vector< vector<string> >& result); 
	
	int setCurrentBinlog(string &name, int position);
	int setCurrentBinlog(int position);
	
	int purgeBinlogTo(string& name); //delete binlog till name(name will not be delete)
	
	int resetBinlog();
	
	string debugString(){
		std::stringstream ss;
		ss << currentBinlogPosition_;
		string posi(ss.str());
		return string(host_ + " " + user_ + " " + passwd_ + " " + dbName_ + " " + currentBinlogName_ + " " + posi);
	}
	
private:
	MYSQL_RES* executeSQL(string &query);

	string host_;
	string user_;
	string passwd_;
	string dbName_;
	unsigned int port_;
	string unixSock_;

	bool hasConnected_;
	
	MYSQL mysql_;
	
	string currentBinlogName_;
	int currentBinlogPosition_;
	deque<string> allBinlogName_;
};





#endif


