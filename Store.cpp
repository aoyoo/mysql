#include <string.h>
#include <string>
#include <stdlib.h> //abort()

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "Store.h"
#include "Logger.h"

using std::vector;
using std::deque;
using std::string;

using namespace boost;

int Store::connect(){
	if(hasConnected_){
		return 0;
	}
	//unixSock seems unused
	//if(!mysql_real_connect(&mysql_, host_.c_str(), user_.c_str(), passwd_.c_str(), dbName_.c_str(), port_, unixSock_.c_str(), 0)){
	if(!mysql_real_connect(&mysql_, host_.c_str(), user_.c_str(), passwd_.c_str(), dbName_.c_str(), port_, 0, 0)){
		LOG_ERROR("Store::connect error " << mysql_error(&mysql_));
		hasConnected_ = false;
		return StoreERR;
	}
	hasConnected_ = true;
	return 0;
}

MYSQL_RES* Store::executeSQL(string &query){
	int ret = mysql_real_query(&mysql_, query.c_str(), query.size());
	if(ret != 0){
		return NULL;
	}
	return (mysql_store_result(&mysql_)); //need free the result outside!!!
}

int Store::getFeature(int id, string &feat){
	stringstream querySS;
	querySS << "select feature from feature where application_id=";
	querySS << id << ";";
	string query = querySS.str();
	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("getFeature query error " << mysql_error(&mysql_));
		return StoreERR;
	}
	while(MYSQL_ROW row = mysql_fetch_row(res)){
		int fieldNum = mysql_num_fields(res);
		if(fieldNum != 1){
			LOG_ERROR("getFeature num != 1" << fieldNum);
			mysql_free_result(res);
			return StoreNumERR; 
		}
		unsigned long* fieldLen = mysql_fetch_lengths(res);
		feat.assign(row[0], fieldLen[0]);
		LOG_DEBUG("getFeature " << row[0] << " len " << fieldLen[0]);
	}
	mysql_free_result(res);
	return 0;
}

int Store::getImage(int id, string &feat){
	stringstream querySS;
	querySS << "select image from feature where application_id=";
	querySS << id << ";";
	string query = querySS.str();
	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("getImage query error " << mysql_error(&mysql_));
		return StoreERR;
	}
	while(MYSQL_ROW row = mysql_fetch_row(res)){
		int fieldNum = mysql_num_fields(res);
		if(fieldNum != 1){
			LOG_ERROR("getImage num != 1" << fieldNum);
			mysql_free_result(res);
			return StoreNumERR; 
		}
		unsigned long* fieldLen = mysql_fetch_lengths(res);
		feat.assign(row[0], fieldLen[0]);
		LOG_DEBUG("getImage " << row[0] << " len " << fieldLen[0]);
	}
	mysql_free_result(res);
	return 0;
}

int Store::getImageURL(int id, string &url){
	stringstream querySS;
	querySS << "select artwork_url_large from feature where application_id=";
	querySS << id << ";";
	string query = querySS.str();
	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("getImageURL query error " << mysql_error(&mysql_));
		return StoreERR;
	}
	while(MYSQL_ROW row = mysql_fetch_row(res)){
		int fieldNum = mysql_num_fields(res);
		if(fieldNum != 1){
			LOG_ERROR("getImageURL num != 1" << fieldNum);
			mysql_free_result(res);
			return StoreNumERR; 
		}
		unsigned long* fieldLen = mysql_fetch_lengths(res);
		url.assign(row[0], fieldLen[0]);
		LOG_DEBUG("getImageURL " << row[0] << " len " << fieldLen[0]);
	}
	mysql_free_result(res);
	return 0;
}


int Store::setFeature(int id, const char* feat, int length){
	//"INSERT INTO feature (application_id, feature) VALUES (id, 'feat') ON DUPLICATE KEY UPDATE feature=VALUES(feature);"
	const char *query = "INSERT INTO feature (application_id, feature) VALUES (?,?) ON DUPLICATE KEY UPDATE feature=VALUES(feature);";
	
	MYSQL_STMT *stmt = mysql_stmt_init(&mysql_);
	if(!stmt){
		LOG_ERROR("mysql_stmt_init() error");
		return -1;
	}
	if(mysql_stmt_prepare(stmt, query, strlen(query))){
		LOG_ERROR("mysql_stmt_prepare insert failed " << mysql_stmt_error(stmt));
		return -1;
	}
	int param_count= mysql_stmt_param_count(stmt);
	if(param_count != 2){
		LOG_ERROR("invalid parameter count returned by MySQL");
		return -1;
	}
	
	MYSQL_BIND bind[2];
	memset(bind, 0, sizeof(bind));
	
	/* This is a number type, so there is no need to specify buffer_length */
	bind[0].buffer_type= MYSQL_TYPE_LONG;
	bind[0].buffer= (char *)&id;
	bind[0].is_null= 0;
	bind[0].length= 0;
	
	unsigned long str_length = length;
	bind[1].buffer_type= MYSQL_TYPE_LONG_BLOB;
	bind[1].buffer= (char *)feat;
	bind[1].buffer_length= length;
	bind[1].is_null= 0;
	bind[1].length= &str_length;
	
	/* Bind the buffers */
	if (mysql_stmt_bind_param(stmt, bind)){
		LOG_ERROR("mysql_stmt_bind_param() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	/* Execute the INSERT statement - 1*/
	if (mysql_stmt_execute(stmt)){
		LOG_ERROR("mysql_stmt_execute() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
	LOG_DEBUG("setFeature affected_rows " << affected_rows << " str_length " << str_length);
	mysql_stmt_close(stmt);
	return 0;

	//stringstream querySS;
	//querySS << ("insert into feature (application_id, feature) values ( ");
	//querySS << id << ", ";
	//querySS.write(feat, length);
	//querySS << ") ON DUPLICATE KEY UPDATE feature=VALUES(feature);";
	//string query = querySS.str();
	//MYSQL_RES* res = executeSQL(query);
	//if(res == NULL){
	//	LOG_ERROR("setFeature query error " << mysql_error(&mysql_));
	//	return StoreERR;
	//}
	//mysql_free_result(res);
	//return 0;
}

int Store::setImage(int id, const char* feat, int length){
	//"INSERT INTO feature (application_id, image) VALUES (id, 'feat') ON DUPLICATE KEY UPDATE image=VALUES(image);"
	const char *query = "INSERT INTO feature (application_id, image) VALUES (?,?) ON DUPLICATE KEY UPDATE image=VALUES(image);";

	MYSQL_STMT *stmt = mysql_stmt_init(&mysql_);
	if(!stmt){
		LOG_ERROR("mysql_stmt_init() error");
		return -1;
	}
	if(mysql_stmt_prepare(stmt, query, strlen(query))){
		LOG_ERROR("mysql_stmt_prepare insert failed " << mysql_stmt_error(stmt));
		return -1;
	}
	int param_count= mysql_stmt_param_count(stmt);
	if(param_count != 2){
		LOG_ERROR("invalid parameter count returned by MySQL");
		return -1;
	}
	
	MYSQL_BIND bind[2];
	memset(bind, 0, sizeof(bind));

	/* This is a number type, so there is no need to specify buffer_length */
	bind[0].buffer_type= MYSQL_TYPE_LONG;
	bind[0].buffer= (char *)&id;
	bind[0].is_null= 0;
	bind[0].length= 0;

	unsigned long str_length = length;
	bind[1].buffer_type= MYSQL_TYPE_LONG_BLOB;
	bind[1].buffer= (char *)feat;
	bind[1].buffer_length= length;
	bind[1].is_null= 0;
	bind[1].length= &str_length;

	/* Bind the buffers */
	if (mysql_stmt_bind_param(stmt, bind)){
		LOG_ERROR("mysql_stmt_bind_param() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	/* Execute the INSERT statement - 1*/
	if (mysql_stmt_execute(stmt)){
		LOG_ERROR("mysql_stmt_execute() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
	LOG_DEBUG("setImage affected_rows " << affected_rows);
	mysql_stmt_close(stmt);
	return 0;
}

int Store::setImageURL(int id, const char* feat, int length){
	//"INSERT INTO feature (application_id, image) VALUES (id, 'feat') ON DUPLICATE KEY UPDATE image=VALUES(image);"
	const char *query = "INSERT INTO feature (application_id, artwork_url_large) VALUES (?,?) ON DUPLICATE KEY UPDATE artwork_url_large=VALUES(artwork_url_large);";

	MYSQL_STMT *stmt = mysql_stmt_init(&mysql_);
	if(!stmt){
		LOG_ERROR("mysql_stmt_init() error");
		return -1;
	}
	if(mysql_stmt_prepare(stmt, query, strlen(query))){
		LOG_ERROR("mysql_stmt_prepare insert failed " << mysql_stmt_error(stmt));
		return -1;
	}
	int param_count= mysql_stmt_param_count(stmt);
	if(param_count != 2){
		LOG_ERROR("invalid parameter count returned by MySQL");
		return -1;
	}
	
	MYSQL_BIND bind[2];
	memset(bind, 0, sizeof(bind));

	/* This is a number type, so there is no need to specify buffer_length */
	bind[0].buffer_type= MYSQL_TYPE_LONG;
	bind[0].buffer= (char *)&id;
	bind[0].is_null= 0;
	bind[0].length= 0;

	unsigned long str_length = length;
	bind[1].buffer_type= MYSQL_TYPE_LONG_BLOB;
	bind[1].buffer= (char *)feat;
	bind[1].buffer_length= length;
	bind[1].is_null= 0;
	bind[1].length= &str_length;

	/* Bind the buffers */
	if (mysql_stmt_bind_param(stmt, bind)){
		LOG_ERROR("mysql_stmt_bind_param() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	/* Execute the INSERT statement - 1*/
	if (mysql_stmt_execute(stmt)){
		LOG_ERROR("mysql_stmt_execute() failed " << mysql_stmt_error(stmt));
		return -1;
	}
	
	my_ulonglong affected_rows = mysql_stmt_affected_rows(stmt);
	LOG_DEBUG("setImageURL affected_rows " << affected_rows);
	mysql_stmt_close(stmt);
	return 0;
}

int Store::getBinlogName(deque<string>& nameVec){
	string query("show binary logs");
	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("getBinlogName query error " << mysql_error(&mysql_));
		return StoreERR;
	}
	
	int binlogFileNum = 0;
	while(MYSQL_ROW row = mysql_fetch_row(res)){
		++binlogFileNum;
		int fieldNum = mysql_num_fields(res);
		//row[0] is binlog name
		//row[1] is binlog size
		//if not 2, error
		if(fieldNum != 2){
			LOG_ERROR("getBinlogName fieldNum " << fieldNum);
			return StoreERR;
		}
		nameVec.push_back(string(row[0]));
	}
	LOG_DEBUG("getBinlogName binlog file num " << binlogFileNum);
	mysql_free_result(res);
	return 0;
}

int Store::getBinlogEvents(const string& logName, int position, int limit, vector< vector<string> >& result){
	string query("show binlog events in ");
	query.append(" '" + logName + "'");
	stringstream posSS;
	posSS << position;
	string positionStr(posSS.str());
	query.append(" from " + positionStr + " ");
	stringstream limitSS;
	limitSS << limit;
	string limitStr(limitSS.str());
	query.append("limit " + limitStr);
	LOG_DEBUG("getBinlogEvents query: " << query);

	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("getBinlogEvents error " << mysql_error(&mysql_));
		return StoreERR;
	}
	
	while (MYSQL_ROW row = mysql_fetch_row(res)){
		vector<string> tmpField;
		int fieldNum = mysql_num_fields(res);
		for(int i = 0; i < fieldNum; ++i){
			tmpField.push_back(string(row[i]));
		}
		result.push_back(tmpField);
	}
	mysql_free_result(res);
	return 0;
}

int Store::getCurrentBinlogName(string& name){
	if(currentBinlogPosition_ == -1 || currentBinlogName_.empty()){
		//has not get a Binlog Name
		LOG_INFO("getCurrentBinlogName & Position");
		allBinlogName_.clear();
		if(getBinlogName(allBinlogName_) != 0){
			return StoreERR;
		}
		currentBinlogName_ = allBinlogName_[0];
		currentBinlogPosition_ = 0;
	}
	name = currentBinlogName_;
	return 0; 
}

int Store::getCurrentBinlogEvents(int limit, vector< vector<string> >& result){
	if(currentBinlogPosition_ == -1 || currentBinlogName_.empty()){
		allBinlogName_.clear();
		if(getBinlogName(allBinlogName_) != 0){
			return -1;
		}
		currentBinlogName_ = allBinlogName_[0];
		currentBinlogPosition_ = 0;
	}
	int ret = getBinlogEvents(currentBinlogName_, currentBinlogPosition_, limit, result);
	if(ret != 0){
		return -1;
	}
	
	if(result.size() == limit){
		vector<string> &lastBinlogVec = result.back();
		if(lastBinlogVec.size() != 6){
			LOG_ERROR("lastBinlogVec size != 6, abort program");
			for(int i = 0; i < lastBinlogVec.size(); i++){
				LOG_ERROR("lastBinlogVec " << lastBinlogVec[i]);
			}
			abort();
		}
		string newPositionStr = lastBinlogVec[4];
		currentBinlogPosition_ = atoi(newPositionStr.c_str());
		return 0;
	}else if(result.empty()){
		LOG_DEBUG("getCurrentBinlogEvents empty");
		return StoreToEnd;
	}
		
	//result size != limit & !result.empty()说明当前的binlog文件读完了，有2种情况
	//1. 如果当前有多个binlog文件(allBinlogName_.size() > 1)：
	//  1.1 正常：如果当前binlog是第一个binlog，则删除currentbinlog，开始读下一个binlog文件
	//  1.2 不正常：如果当前binlog不是第一个binlog，严重错误，退出程序，检查问题。
	//
	//2. 如果当前只有这一个binlog文件：
	//根据最后一个log中判断出是否开始写下一个binlog：
	//   如果是，则删除currentBinlog，从最后一个log得到一下个binlog的名字，开始读下一个
	//   如果不是，则当前的binlogName里所有binlog读完，等待。
	if(allBinlogName_.size() > 1){
		if(currentBinlogName_ == allBinlogName_[0]){
			currentBinlogName_ = allBinlogName_[1];
			currentBinlogPosition_ = 0;
			purgeBinlogTo(currentBinlogName_);
			allBinlogName_.pop_front();
			return 0;
		}else{
			LOG_ERROR("currentBinlog is not the first Binlog, abort program");
			abort();
		}
	}else{
		//当前只有这一个binlog文件
		vector<string> &nextBinlogVec = result.back();
		//nextBinlogVec maybe as
		//master-bin.000002
		//106
		//Rotate
		//1
		//150
		//master-bin.000003;pos=4
		if(nextBinlogVec.size() != 6){
			LOG_ERROR("nextBinlogVec size != 6, abort program");
			for(int i = 0; i < nextBinlogVec.size(); i++){
				LOG_WARN("nextBinlogVec " << nextBinlogVec[i]);
			}
			abort();
		}
		
		//Rotate means the master switches to a new log file 
		if(strcmp(nextBinlogVec[2].c_str(), "Rotate") == 0){
			//get the next binlog name & position
			string newBinlogStr = nextBinlogVec[5];
			vector<string> newBinlogVec;
			split(newBinlogVec, newBinlogStr, is_any_of(";="), token_compress_on);
			if(newBinlogVec.size() != 3){
				LOG_ERROR("split newBinlogStr error " << newBinlogStr << " " << newBinlogVec.size() << " abort program");
				abort();
			}
			if(strcmp(newBinlogVec[1].c_str(), "pos") != 0){
				LOG_ERROR("split newBinlogStr error " << newBinlogStr << " " << newBinlogVec.size() << " abort program");
				abort();
			}
			currentBinlogName_ = string(newBinlogVec[0]);
			currentBinlogPosition_ = atoi(newBinlogVec[2].c_str());
			purgeBinlogTo(currentBinlogName_);
			return StoreToEnd;
		}else{
			//the last log is not switches to a new log file
			//binlog read to end
			if(strcmp(nextBinlogVec[2].c_str(), "Query") != 0)
				LOG_DEBUG("nextBinlogVec[2] " << nextBinlogVec[2]);
			
			vector<string> &lastBinlogVec = result.back();
			if(lastBinlogVec.size() != 6){
				LOG_ERROR("lastBinlogVec size != 6, abort program");
				for(int i = 0; i < lastBinlogVec.size(); i++){
					LOG_ERROR("lastBinlogVec " << lastBinlogVec[i]);
				}
				abort();
			}
			string newPositionStr = lastBinlogVec[4];
			currentBinlogPosition_ = atoi(newPositionStr.c_str());
			return StoreToEnd; //TODO make sure this
		}
		return 0;
	}
	LOG_ERROR("what happend...");
	return -1;
}

int Store::setCurrentBinlog(string &name, int position){
	currentBinlogName_ = name;
	currentBinlogPosition_ = position;
	return 0;
}

int Store::setCurrentBinlog(int position){
	currentBinlogPosition_ = position;
	return 0;
}

int Store::purgeBinlogTo(string &name){
	//PURGE {MASTER | BINARY} LOGS TO 'log_name'
	string query("PURGE BINARY LOGS TO '" + name + "';");
	MYSQL_RES* res = executeSQL(query);
	if(res == NULL){
		LOG_ERROR("purgeBinlogTo " << name << " error " << mysql_error(&mysql_));
		return -1;
	}
	mysql_free_result(res);
	return 0;
}

int Store::resetBinlog(){
	//just use after some error happend
	currentBinlogName_.clear();
	currentBinlogPosition_ = -1;
	return 0;
}



