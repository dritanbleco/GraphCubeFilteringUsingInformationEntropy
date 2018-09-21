package org.apache.spark.examples.sql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object RDDEtropyFiltering
	{
		  private final val MAX_LENGTH=6;
		  
		  var number = new Array[Int](MAX_LENGTH);
		  private final var arrString= new Array[String](MAX_LENGTH);
		  //private final var m:Int=0;
		  private final var t:Int=0;
		  private final var bool_t:Int=0;
		  private final var temp:Int=0;
		  private final var qryString= new Array[String](Math.pow(2,MAX_LENGTH).toInt);
		  private final var rel_parent= new Array[Int](Math.pow(2,MAX_LENGTH).toInt);
        def main(args: Array[String])
		{
			arrString(0)="start_gender";
			arrString(1)="start_nation";
			arrString(2)="str_profession";
			
			arrString(3)="end_gender";
			arrString(4)="end_nation";
			arrString(5)="end_profession";
			 
	
			
			var t1 = System.currentTimeMillis
			var tstart = System.currentTimeMillis
			var t2 = System.currentTimeMillis
			val spark = SparkSession
  				.builder()
  				.master("spark://master:7077")
  				.appName("Spark SQL entropy")
  				.getOrCreate()
  			spark.conf.set("spark.executor.memory", "8g")
			import spark.implicits._
			val edges = spark.read.textFile("relDhead").map(_.split("[|]",-1)).filter(l => l.length > 1).map(l => Link(l(0).trim.toInt, l(1).trim.toInt))
			val nodes = spark.read.textFile("nodeDhead").map(_.split("\t",-1)).filter(l => l.length > 4).map(c => Attributes(c(0).trim.toInt, c(1).trim ,c(2).trim ,c(3).trim))
			edges.createOrReplaceTempView("edgeT")
			nodes.createOrReplaceTempView("nodeT")
			var sqlDF = spark.sql("SELECT nodeA,nodeB,Start.gender start_gender,Start.nation start_nation,Start.profession str_profession,End.gender end_gender,End.nation end_nation,End.profession end_profession FROM edgeT,nodeT Start,nodeT End where Start.node=edgeT.nodeA and End.node=edgeT.nodeB")
			sqlDF.createOrReplaceTempView("myData")
			sqlDF=spark.sql("SELECT count(*)cnt,start_gender,start_nation,str_profession,end_gender,end_nation,end_profession FROM myData group by start_gender,start_nation,str_profession,end_gender,end_nation,end_profession") ;
			sqlDF.createOrReplaceTempView("myData")
			sqlDF.cache
			sqlDF.show()
			int_buffer_rec(MAX_LENGTH, MAX_LENGTH);
			 for(i <-qryString.length-2 to 0 by -1 )  {
				
			         sqlDF = spark.sql(qryString(i));
				 sqlDF.createOrReplaceTempView("myData1");
				 sqlDF.show()
				 
			} 
			t2 = System.currentTimeMillis;
			  
            }
            
            
	    
	    def int_buffer_rec( n:Int,  length:Int) {
	    		var sql:String="";
	    		var a: String="";
	    		var gr: String="";
			if(n > 0) {
			    number(MAX_LENGTH-n) = 0;
			    int_buffer_rec(n - 1, length);
			    number(MAX_LENGTH-n) = 1;
			    int_buffer_rec(n - 1, length);
			}
			else {
				sql="Select sum(cnt)cnt";
			    for(i <-0 to  length-1) {
			    	if(number(i)==1){
			    		a=a+","+arrString(i) ;
			    		t=t+Math.pow(2,i).toInt;
			    	}else{
			    	 if(bool_t==0){
			    	 	temp=Math.pow(2,i).toInt;
			    	 	bool_t=1;
			    	 }
			    	}
				}
			    if(a.length()>0)
			    	gr=" group by "+a.substring(1);
			    	qryString(t)=sql+a+" from myData "+gr;
			    	rel_parent(t)=t+temp;
			    bool_t=0;
			    temp=0;
			    t=0;
		}
		}
		case class Link(gId: Int,nodeA: Int, nodeB: Int)
		case class Attributes(node: Int, gender: String, nation: String, profession: String)
		
   }
