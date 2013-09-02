#include <string>
#include <vector>

#include <stdlib.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/regex.hpp>
#include <boost/algorithm/string/find.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/erase.hpp>
#include <boost/algorithm/string/trim.hpp>
 
#include "Store.h"
#include "Logger.h"

using namespace std;
using namespace boost;

string host = "localhost";
//string user = "epfimporter";
//string passwd = "epf123";
string user = "root";
string passwd = "";
string db_name = "apphunter";
string table_name = "epf_application";
string sock_file = "/var/lib/mysql/mysql.sock";

const string startStr("use `epf`;");
const string VALUES("VALUES"); 
const string INSERT("INSERT"); 
const string REPLACE("REPLACE"); 
const string epf_application_tmp("epf_application_tmp"); 
const string epf_application("epf_application"); 
const string epf_application_inc("epf_application_inc"); 

const string FULL_UPDATA("use `epf`; INSERT  INTO epf_application_tmp (export_date, application_id, title, recommended_age, artist_name, seller_name, company_url, support_url, view_url, artwork_url_large, artwork_url_small, itunes_release_date, copyright, description, version, itunes_version, download_size) VALUES");

const string INCR_UPDATA1("use `epf`; REPLACE  INTO epf_application (export_date, application_id, title, recommended_age, artist_name, seller_name, company_url, support_url, view_url, artwork_url_large, artwork_url_small, itunes_release_date, copyright, description, version, itunes_version, download_size) VALUES");

const string INCR_UPDATA2("use `epf`; INSERT  INTO epf_application_inc (export_date, application_id, title, recommended_age, artist_name, seller_name, company_url, support_url, view_url, artwork_url_large, artwork_url_small, itunes_release_date, copyright, description, version, itunes_version, download_size) VALUES"); 

int analyzeResult(vector<vector<string> >& result);

int main(int argc, char **argv)
{
	VSLogger::Init("log.conf", "main");
	LOG_INFO("test server starting...");
	Store db(host, user, passwd, db_name, 0, sock_file);
	int ret = db.connect();
	if(ret != 0){
		LOG_ERROR("Store connect error");
		return -1;
	}
	
	char buf[1024] = "abcdefg";
	cout << db.debugString() << endl;
	db.setFeature(11111, buf, 4);
	db.setImage(11111, buf, 4);
	
	string featureContent;
	db.getFeature(11111, featureContent);
	db.getImage(11111, featureContent);
	db.getFeature(1, featureContent);

/*	
	{
		vector<vector<string> > result;
		int logNum = 10;
		int count = 0;
		while(1){
			++count;
			LOG_INFO("getCurrentBinlogEvents count " << count);
			result.clear();
			ret = db.getCurrentBinlogEvents(logNum, result);
			if(ret == StoreToEnd){
				analyzeResult(result);
				LOG_DEBUG("Store getCurrentBinlogEvents to the end");
				sleep(5);
				continue;
			}else if(ret == StoreERR){
				LOG_ERROR("StoreERR getCurrentBinlogEvents ERROR");
				abort();
			}else{
				analyzeResult(result);
			}
		}
	}
*/
	return 0;
}

int analyzeResult(vector<vector<string> >& result){
	
	for(int i = 0; i < result.size(); ++i){
		int binlogQuerySize = result[i].size();
		if(binlogQuerySize != 6){
			LOG_ERROR("binlogQuerySize " << binlogQuerySize);
			for(int j = 0; j < binlogQuerySize; ++j){
				LOG_ERROR("result " << i << " " << j << " " << result[i][j]);
			}
			abort();
		}
		string &line = result[i][5];
		string SQLStr;
		if(istarts_with(line, FULL_UPDATA)){
			SQLStr = line.substr(FULL_UPDATA.size());
		}else if(istarts_with(line, INCR_UPDATA1)){
			SQLStr = line.substr(INCR_UPDATA2.size());
		}else if(istarts_with(line, INCR_UPDATA1)){
			SQLStr = line.substr(INCR_UPDATA2.size());
		}else{
			LOG_INFO("get what: " << line);
			continue;
		}
		trim(SQLStr);
		if( (starts_with(SQLStr, "('") && ends_with(SQLStr, "')")) || 
				(starts_with(SQLStr, "('") && ends_with(SQLStr, "NULL)")) ){
		
			if(starts_with(SQLStr, "('") && ends_with(SQLStr, "')")){
				erase_first(SQLStr, "('");
				erase_last(SQLStr, "')");
			}else{
				erase_first(SQLStr, "('");
				erase_last(SQLStr, ")");
			}
			trim(SQLStr);
			vector<std::string> vStr;
			const boost::regex re("'\\), \\('");
			split_regex(vStr, SQLStr, re);
			int i = 0;
			for(vector<string>::iterator it = vStr.begin(); it != vStr.end(); ++it, ++i){
				vector<string> subStrVec;
				split(subStrVec, *it, is_any_of(" ,'"), token_compress_on);
				//cout << "1 " << subStrVec[0] << " 2 " << subStrVec[1] << " 3 " << subStrVec[2] << endl;
				if(subStrVec.size() < 17){ //17 is number of table epf_application
					LOG_ERROR("subStrVec size " << subStrVec.size() << " STR " << *it);
					abort();
				}
				LOG_INFO("get app_id " << subStrVec[1]);
			}
		}else{
			LOG_ERROR("starts or ends error " << SQLStr);
			continue; //FIXME 在getBinlogEvents时候position已经修改为返回的所有结果之后了，所以外面必须处理完所有的请求！
			//abort(); //FIXME 在getBinlogEvents时候position已经修改为返回的所有结果之后了，所以外面必须处理完所有的请求！
		}
	}
}



//	{
//		vector<string> result;
//		ret = db.getBinlogName(result);
//		if(ret != 0){
//			LOG_ERROR("Store getBinlogName error");
//			return -1;
//		}
//		cout << result[0] << endl;
//	}

//	{
//		string result;
//		ret = db.getCurrentBinlogName(result);
//		if(ret != 0){
//			LOG_ERROR("Store getCurrentBinlogName error");
//			return -1;
//		}
//		cout << result << endl;
//	}
	
