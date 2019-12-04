package my_mysql


import (
    //"fmt"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "gomyenv"
    "errors"
    "sort"
)

type resultSet [][]string

const (
    MAX_VERSION_IDX = 9999
)

func GetVersionIdx(columns []string)(version_idx int){
    version_idx = MAX_VERSION_IDX
    for idx:=len(columns)-2;idx>=0;idx--{
        if columns[idx]=="__version" && columns[idx+1]=="__deleted"{
            version_idx = idx
            break
        }
    }
    return version_idx
}

//rows1 is mysql rows2 is myshard
func RowsCompare(rows1 *sql.Rows,rows2 *sql.Rows)(int,error){
  	var (
        err 	        error
        diff            int64
        rows_num        int64
        columns1        []string
        columns2        []string
        values1         []sql.RawBytes
        values2         []sql.RawBytes
        args1  	        []interface{}
        args2 	        []interface{}
        result1         resultSet
        result2         resultSet
        __version_idx   int
  	)
    __version_idx = MAX_VERSION_IDX
    columns1,err = rows1.Columns()
    gomyenv.CheckNil(err)
    columns2,err = rows2.Columns()
    gomyenv.CheckNil(err)
    if len(columns1)!=len(columns2){
        //fmt.Println("columns1",columns1)
        //fmt.Println("columns2",columns2)
        if len(columns1)+2==len(columns2){
            //myshard more than mysql:when mysql no __version and select nohash table
            //find myshard version
            __version_idx = GetVersionIdx(columns2)
        }else if len(columns1)==len(columns2)+2{
            //mysql more than myshard:when mysql has __version and select hash table
            //find mysql version
            __version_idx = GetVersionIdx(columns1)
        }else{
            return -1,errors.New("column not same")
        }
        //fmt.Println("__version_idx",__version_idx)
        if __version_idx>=MAX_VERSION_IDX{
           return -1,errors.New("column not same,no find __version") 
        }
    }

    values1 = make([]sql.RawBytes, len(columns1))
    values2 = make([]sql.RawBytes, len(columns2))
    args1 = make([]interface{},len(values1))
    args2 = make([]interface{},len(values2))
    for i := range values1 {
        args1[i] = &values1[i]
    }
    for i := range values2 {
        args2[i] = &values2[i]
    }
    for rows1.Next(){
        err = rows1.Scan(args1...)
        if err != nil {
            panic(err)
        }
        if !rows2.Next(){
            //fmt.Println("rows_num",rows_num)
        	return -1,errors.New("rows1 more than rows2")
        }
        err = rows2.Scan(args2...)
        if err != nil {
            panic(err)
        }

        var (
            column_list1  []string
            column_list2  []string
            less_values   *[]sql.RawBytes = &values1
            more_values   *[]sql.RawBytes = &values2
        )
        //use the less values to for,myshard or mysql maybe has __deleted,see above
        if len(columns1)>len(columns2){
            less_values = &values2
            more_values = &values1
        }
        for i := range *less_values{  		    
            string1 := string((*less_values)[i])
            var string2 string
            //for the case: select *,sum(x) will extend as a,b,__version,__deleted,sum(x)
            if i>=__version_idx{
                string2 = string((*more_values)[i+2])
                //fmt.Println("get mapping string1",string1,"string2",string2)
            }else{
                string2 = string((*more_values)[i])
            }
            column_list1 = append(column_list1,string1)
            column_list2 = append(column_list2,string2)
            
            //fmt.Println("string1",string1)
            //fmt.Println("string2",string2)
  	  	    if string1!=string2{
                //fmt.Println("i",i,"v1",values1[i],"v1len",len(values1[i]),"v2",values2[i],"v2len",len(values2[i]))
  	  	  	    //fmt.Println("i",i,"s1",string1,"s1len",len(string1),"s2",string2,"s2len",len(string2))
  	  	  	    //return errors.New(columns1[i]+" values not same")
                //select sum(red_diamond) from user_day_red_diamond where uid = 3615394274 and day >= 20190125;
                //sum(red_diamond)  sum(red_diamond) 
                //206.00            206 
                if( (string1+".00"!=string2) && !(len(string1)==0&&string2=="0.00") ){
                    diff += 1
                }
  	  	    }  
        }
        result1 = append(result1,column_list1)
        result2 = append(result2,column_list2)
        rows_num += 1
    }
    if rows2.Next(){
        //fmt.Println("rows_num",rows_num)
        return -1,errors.New("rows2 more than rows1")
    }
    if diff>0{
        //fmt.Println("result1",result1)
        //fmt.Println("result2",result2)
        sort.Sort(result1)
        sort.Sort(result2)
        //fmt.Println("result1",result1)
        //fmt.Println("result2",result2)
        if len(result1)==len(result2){
            for idx_row,row := range result1{
                for idx_col,col := range row{
                    if col!=result2[idx_row][idx_col]{
                        return -1,errors.New("sorted values not same["+col+"]["+result2[idx_row][idx_col]+"]") 
                    }
                }
            }
        }else{
            return 1,errors.New("sorted rows not same") 
        }
    }
    return 0,nil
}
func (this resultSet)Len()int{
    return len(this)
}
func (this resultSet)Less(i,j int)bool{
    var idx int
    for idx,_ = range this[i]{
        if this[i][idx] != this[j][idx]{
            break
        }
    }
    return this[i][idx] < this[j][idx]
}
func (this resultSet)Swap(i,j int){
     this[i],this[j] = this[j],this[i]
}


