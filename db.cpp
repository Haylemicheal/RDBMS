#include<iostream>
#include<vector>
#include<string>
#include <boost/algorithm/string.hpp>
#include<map>
#include<set>
#include <iterator> 
class Rdbms{
    public:
        typedef void (*FnPtr)(Rdbms& obj);
        std::string str;
        std::vector<std::string> results;
        std::vector<std::string> dbIndex;
        std::vector<std::string> params;
        std::map<std::string,FnPtr> mymap;
        std::set<std::vector<std::string>> table[5];
        std::set<std::vector<std::string>> intersect;
        std::vector<std::string> tuples;
        std::vector<std::string> values;   
        std::vector<std::string> table_names;  
        std::vector<std::string> table1_data;  
        std::vector<std::string> table2_data;  
        std::vector<std::string> cols;  
};

void create_database(Rdbms &obj){
    obj.dbIndex.push_back(obj.results.at(2));
    std::cout<<"New Database created!"<<std::endl;
}
void create_table(Rdbms &obj){
    obj.tuples.push_back(obj.dbIndex.at(0));
    obj.tuples.push_back(obj.results.at(2));
    obj.table_names.push_back(obj.results.at(2));
    int index = obj.table_names.size() - 1;
    obj.table[index].insert(obj.tuples);
    obj.tuples.clear();
    for(std::vector<std::string>::iterator itr=obj.params.begin();itr!=obj.params.end();itr++){
        obj.tuples.push_back(*itr);
    }
    obj.table[index].insert(obj.tuples);
    obj.tuples.clear();
    obj.params.clear();
    std::cout<<"New table created!"<<std::endl;  
}
void insert_into(Rdbms &obj){
    int index;
    for(int x=0;x<obj.table_names.size();x++){
        if(obj.table_names.at(x).compare(obj.results.at(2))==0){
            index = x;
            break;
        }
    }
    for(int i=0;i<obj.values.size();i++)
        obj.tuples.push_back(obj.values.at(i));
    obj.table[index].insert(obj.tuples);
    obj.tuples.clear();
    obj.values.clear();
    obj.params.clear();
    std::cout<<"New record inserted!"<<std::endl;
}
int max(int x,int y){
    return x > y ? x : y;
}
void inner_join(Rdbms &obj,int index1,int index2){
    std::set<std::vector<std::string>>::iterator it,st,sit;
    std::set_intersection(obj.table[0].begin(),obj.table[0].end(), obj.table[1].begin(), obj.table[1].end(), std::inserter(obj.intersect,obj.intersect.begin())); 
    st = obj.intersect.begin();
    it = obj.intersect.end();
    sit = it;
    --sit;
    for(st;st!=it;++st){
        for(int i=0;i<(*st).size();i++){
            if(i%(*st).size()==0)
                std::cout<<std::endl;
            std::cout<<(*sit)[i]<<"\t";
        }
        if(sit!=obj.intersect.begin())
            --sit;
         std::cout<<std::endl;
        }
}
void left_join(Rdbms &obj,int index1,int index2){
    std::set< std::vector<std::string> >::iterator it;
    it = obj.table[index1].begin();
    auto end = obj.table[index1].end();
    --end;--end;
    int tabsize = obj.table[index1].size() - 2;
    for(tabsize;tabsize>=0;tabsize--){
            for(int j=0;j<(*end).size();j++){
                std::cout << (*end)[j] << "\t"; 
            }
            end--;
            std::cout <<std::endl;
    }
    inner_join(obj,index2,index1);
}
void right_join(Rdbms &obj,int index1,int index2){
    std::set< std::vector<std::string> >::iterator it;
    it = obj.table[index2].begin();
    auto end = obj.table[index2].end();
    --end;--end;
    int tabsize = obj.table[index2].size() - 2;
    for(tabsize;tabsize>=0;tabsize--){
            for(int j=0;j<(*end).size();j++){
                std::cout << (*end)[j] << "\t"; 
            }
            end--;
            std::cout <<std::endl;
    }
    inner_join(obj,index1,index2);
}
void full_join(Rdbms &obj,int index1,int index2){
    std::set<std::vector<std::string>>::iterator it;
    it = obj.table[index1].begin();
    auto end1 = obj.table[index1].end();
    --end1;--end1;
    int tabsize = obj.table[index1].size() - 2;
    for(tabsize;tabsize>=0;tabsize--){
            for(int j=0;j<(*end1).size();j++){
                std::cout << (*end1)[j] << "\t"; 
            }
            end1--;
            std::cout <<std::endl;
    }
    
    it = obj.table[index2].begin();
    auto end2 = obj.table[index2].end();
    --end2;--end2;
    tabsize = obj.table[index2].size() - 2;
    for(tabsize;tabsize>=0;tabsize--){
            for(int j=0;j<(*end2).size();j++){
                std::cout << (*end2)[j] << "\t"; 
            }
            end2--;
            std::cout <<std::endl;
    }
}


/*
void cross_join(Rdbms obj,int index1,int index2){
    std::set< std::vector<std::string> >::iterator it1,it2;
    it1 = obj.table[index1].begin();
    auto end1 = obj.table[index1].end();
    --end1;--end1;
    int tabsize1 = obj.table[index1].size() - 2;
    int tabsize2 = obj.table[index2].size() - 2;
    it2 = obj.table[index2].begin();
    auto end2 = obj.table[index2].end();
    --end2;--end2;
    int maxsize = max(tabsize1,tabsize2);
    
}*/
void joins(Rdbms &obj){
    int index1,index2,max_size;
    boost::split(obj.cols, obj.results.at(7), [](char c){return c == '=';});
    boost::split(obj.table1_data, obj.cols.at(0), [](char c){return c == '.';});
    boost::split(obj.table2_data, obj.cols.at(1), [](char c){return c == '.';});
    for(int x=0;x<obj.table_names.size();x++){
            if(obj.table_names.at(x).compare(obj.table1_data[0])==0){
                index1 = x;
                continue;
            }
            else if(obj.table_names.at(x).compare(obj.table2_data[0])==0){
                index2 = x;
                continue;
            }
    }

    std::string inner= "inner join";
    std::string left= "left join";
    std::string right= "right join";
    std::string full= "full join";
    std::string command = obj.results.at(3)+" "+obj.results.at(4);
    if(inner.compare(command)==0){
        inner_join(obj,index1,index2);
    }
    else if(left.compare(command)==0){
        left_join(obj,index1,index2);
    }
    else if(right.compare(command)==0){
        right_join(obj,index1,index2);
    }
    else if(full.compare(command)==0){
        full_join(obj,index1,index2);
    }
    else{
        std::cout<<"unknown command!";
    }
}

void select_from(Rdbms &obj){
    if(obj.results.size()<=3){
        int index;
         for(int x=0;x<obj.table_names.size();x++){
            if(obj.table_names.at(x).compare(obj.results.at(2))==0){
                index = x;
                break;
            }
        }
        std::set< std::vector<std::string> >::iterator it;
        it = obj.table[index].begin();
        auto end = obj.table[index].end();
        --end;--end;
        int tabsize = obj.table[index].size() - 2;
        if(obj.values.size()>0)
            boost::split(obj.values, obj.values.at(0), [](char c){return c == ' ';});
        if(obj.params.at(0).compare("*")==0){
            for(tabsize;tabsize>=0;tabsize--){
                for(int j=0;j<(*end).size();j++){
                    std::cout << (*end)[j] << "\t"; 
                }
                end--;
                std::cout <<std::endl;
           }
        }
        else{
            std::vector<int> indexes;
            int colsize = (*end).size();
            for(int j=0;j<obj.params.size();j++){
            for(int i=0;i<colsize;i++){
                if((*end)[i].compare(obj.params.at(j))==0){
                    indexes.push_back(i);
                }            
            }
          }
             for(tabsize;tabsize>=0;tabsize--){
                for(int x:indexes){
                    std::cout << (*end)[x] << "\t"; 
                }
                end--;
                std::cout <<std::endl;
            }
        }
        obj.params.clear();
        obj.values.clear();
    }
    else{
        joins(obj);
    }
}
int main(){
    Rdbms obj;
    while(1){
    std::cout<<"RDMS:";
    std::getline(std::cin,obj.str);
    if (obj.str.find("|") != std::string::npos){
        boost::split(obj.results, obj.str, [](char c){return c == '|';});
        if (obj.results.at(1).find(",") != std::string::npos)
            boost::split(obj.params, obj.results.at(1), [](char c){return c == ',';});
        else
            obj.params.push_back(obj.results.at(1));
        if(obj.results.size()>2){
            if (obj.results.at(2).find(",") != std::string::npos)
                boost::split(obj.values, obj.results.at(2), [](char c){return c == ',';});
            else
                obj.values.push_back(obj.results.at(2));
        }
        boost::split(obj.results, obj.results.at(0), [](char c){return c == ' ';});
    }
    else{
    boost::split(obj.results, obj.str, [](char c){return c == ' ';});
    }
    obj.mymap["create database"] = create_database;
    obj.mymap["create table"] = create_table;
    obj.mymap["insert into"] = insert_into;
    obj.mymap["select from"] = select_from;
    obj.mymap[obj.results.at(0)+" "+obj.results.at(1)](obj);
    }
    return 0;
}
