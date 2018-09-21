package org.apache.spark.examples.sql
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._
import Array._


object RDDCubeMaterialization 
	{
		var NumberOfAttributes:Int=6; // Define here the number of attributes
		var NumberOfCombinations:Int=scala.math.pow(2,NumberOfAttributes).ceil.toInt;
		var all_comb_binary = ofDim[Boolean](NumberOfCombinations,NumberOfAttributes);
		var table_comb_int:Array[Int]=new Array[Int](NumberOfCombinations);
		var index:Int =0;
		
		def main(args: Array[String]) 
		{
			val t1 = System.currentTimeMillis
			var t2 = System.currentTimeMillis
            		val conf = new SparkConf() // Define here the configuration of Spark
                         .setMaster("spark://hadoopmanager:7077")
                         .setAppName("My application")
                        val sc = new SparkContext(conf)
			val sqlContext = new SQLContext(sc);
			var found:Boolean=true;
			var notfound:Boolean=true;
			var notAllFoundCub:Boolean=true;
			var notCutted=true;
			var arrayY  : ArrayBuffer[Int] = ArrayBuffer();
			var arrayN  : ArrayBuffer[Int] = ArrayBuffer();
			var resultN  : ArrayBuffer[String] = ArrayBuffer();
			var intResultN  : ArrayBuffer[Int] = ArrayBuffer();
			
			var maxThreadCNT:Int=args(0).toInt;
			var MaxHop:Int=args(1).toInt;
			var step:Int=0;
			var temp:Int=0;
			var graphLinks  : Array[org.apache.spark.sql.SchemaRDD]=new Array[org.apache.spark.sql.SchemaRDD](MaxHop);
			
			import org.apache.spark.sql._
			import sqlContext.createSchemaRDD
		
			// Read the graph relations
			val graph = sc.textFile("graph_"+args(2)+".txt").map(_.split(",")).map(l => Link(l(0).trim.toInt, l(1).trim.toInt,l(2).trim.toInt))
			graph.registerTempTable("graph")
			
			//Read the attributes of nodes
			val dat = sc.textFile("dat.txt").map(_.split(",")).map(c => Attributes(c(0).trim.toInt, c(1) ,c(2) ,c(3)))
			dat.registerTempTable("char")
			
			//Create the table associating attributes for Starting Nodes
			val bas = sqlContext.sql("SELECT nodeA,nodeB,char.gender,char.nation,char.profession,gId FROM char,graph where nodeA=char.node")
			bas.registerTempTable("basAnode")

			//Create the Basic One hop table associating attributes for Starting and Ending Nodes
			var basicTable = sqlContext.sql("SELECT nodeA,nodeB,basAnode.gender genderA,basAnode.nation nationA,basAnode.profession professionA,char.gender genderBT,char.nation nationBT,char.profession professionBT, gId FROM char, basAnode where nodeB=char.node")
			basicTable.registerTempTable("basicTable")
			generate_tree(NumberOfAttributes);
			
			var resultMap = collection.mutable.Map[String, Int]();
			var notMap = collection.mutable.Map[String, Int]();
			var vbTab:Array[Boolean]=Array();
			var int_cuboid:Int=0;
			
			var all_comb_binary_ordered = order_binary_table(all_comb_binary,table_comb_int);
			graphLinks(0) = sqlContext.sql("SELECT nodeA,nodeB,genderA,nationA,professionA,genderBT As genderB,nationBT As nationB, professionBT As professionB,gId FROM basicTable")
			graphLinks(0).registerTempTable("graphLinks"+0)
            		for( i <- 1 until MaxHop)
			{
				
  				graphLinks(i) = sqlContext.sql("SELECT A.nodeA,B.nodeB,A.genderA,A.nationA,A.professionA,B.genderBT as gender"+i+",B.nationBT as nation"+i+",B.professionBT as profession"+i+",A.nodeB n2,B.nodeA n1,A.gId FROM graphLinks"+(i-1)+" A inner join basicTable B on A.nodeB=B.nodeA and A.gId=B.gId")
				graphLinks(i).registerTempTable("graphLinks"+i);
                
			}
			var pairs=graphLinks(0).map(s => ("*,*,*-*,*,*"  ,1)) ;
			
			
			
			for( i <- 0 until all_comb_binary_ordered.length)
      			{ 
      				vbTab=all_comb_binary_ordered(i);
      				int_cuboid=bool_to_int(vbTab);
				breakable 
				{ 	
					notCutted=true;
					for((element) <-arrayN)
					{
						if((element & int_cuboid)== element)
						{
							notCutted=false;
							break;
						}
					}
				}
				if(notCutted)
				{
					notAllFoundCub=false;
					found=true;
					notfound=true;
					for( step <- 0 until MaxHop)
					{
						breakable 
						{ 
							if(!notAllFoundCub)
							{
								notAllFoundCub=true;
								pairs = graphLinks(step).map(s => (create_attr_group(vbTab,resultMap,resultN,intResultN,s,int_cuboid),1)).filter(_._1!="NOT") ;
								for ((str, cnt) <- pairs.reduceByKey((a, b) => a + b).collect)
								{
                                     				if(!resultMap.contains(str))
									{
										temp=cnt;
										if(step>0)
											if(notMap.contains(str))
												temp=notMap(str)+temp;
										
										if(temp>maxThreadCNT)
										{
											resultMap=resultMap+(str -> temp);						
											notfound=false;
										}
										else{
											notAllFoundCub=false;
											notMap=notMap+(str -> temp);
											if(step==(MaxHop-1))
											{
												found=false;
												intResultN+:=int_cuboid;
												resultN+:=str;
											}									
										}
									}
								}
							}else
								break;
						}
					}
					if(found)
						arrayY+:=int_cuboid;
					if(notfound)
						arrayN+:=int_cuboid;
				}
      			}
      			
		}
		
		
		
		def generate_tree( n:Int ) = 
		{
			var res:Array[Boolean] = new Array[Boolean](n);
			generate_binary(res, 0);
        	}
		
		def generate_binary(res:Array[Boolean],start:Int) {
            		if (start == res.length) {
                		var m:Int = 0;
				for( j:Int <- 0 to res.length-1)
                		{
				    all_comb_binary(index)(j) = res(j);
				    if (res(j))
					m=m+1;

                		}
				table_comb_int(index) = m;
				index=index+1;
            		} else 
			{
				generate_binary(res, start + 1);
				res(start) = true;
				generate_binary(res, start + 1);
				res(start) = false;
            		}
        	}
		
		def order_binary_table( all_comb_binary_local :Array[Array[Boolean]],table_comb_int_local:Array[Int] ):Array[Array[Boolean]] = 
		{
			var res = ofDim[Boolean](scala.math.pow(2,NumberOfAttributes).ceil.toInt,NumberOfAttributes);
			var counter:Int=1;
			res(0)=all_comb_binary_local(0);
			for ( i :Int <- 1 until all_comb_binary_local(0).length)
				for ( j:Int <- 1 until all_comb_binary_local.length)
					if (table_comb_int_local(j) == i)
						{
							res(counter)=all_comb_binary_local(j);
							counter=counter+1;
						}
			res(all_comb_binary_local.length-1)=all_comb_binary_local(all_comb_binary_local.length-1);
			return res
        	}
		
		def create_attr_group(arr:Array[Boolean],resMap:collection.mutable.Map[String, Int],resN:ArrayBuffer[String],intResN:ArrayBuffer[Int],str:org.apache.spark.sql.Row, int_cub:Int) :String = 
		{
			var result:String=""
			var nameValuePairs: Array[String] = Array()
			var str1:Array[String] = Array()
			var str2: Array[String] = Array()
			result= {if(arr(0) ) str(2)+"," else "*,"}+""
			result= result+ {if(arr(1)) str(3)+"," else "*,"}
			result= result+ {if(arr(2)) str(4)+"-" else "*-"}
			result= result+ {if(arr(3)) str(5)+"," else "*,"}
			result= result+ {if(arr(4)) str(6)+"," else "*,"}
			result= result+ {if(arr(5)) str(7)+"" else "*"}
			
			if(resMap.contains(result)){
				return "NOT";
			}
			for( j:Int <- 0 until intResN.length)
				if((int_cub & intResN(j))== int_cub)
				{
					nameValuePairs = resN(j).toString.split("-")
					str1=nameValuePairs(0).split(",")
					str2=nameValuePairs(1).split(",")
					if(
						(!arr(0)||(arr(0)&& str1(0)==str(2)))&& 
						(!arr(1)||(arr(1)&& str1(1)==str(3)))&&
						(!arr(2)||(arr(2)&& str1(2)==str(4)))&&
						(!arr(3)||(arr(3)&& str2(0)==str(5)))&&
						(!arr(4)||(arr(4)&& str2(1)==str(6)))&&
						(!arr(5)||(arr(5)&& str2(2)==str(7)))
					) 
					return "NOT";
				}
			return  result;
		}
		def bool_to_int(arr:Array[Boolean]) :Int = 
		{
			var temp:Int =0;
			for ( j:Int <- 0 until arr.length)
                if(arr(j)) temp=temp+scala.math.pow(2,j).ceil.toInt;
			return temp;
		}
		case class Link(gId: Int,nodeA: Int, nodeB: Int)
		case class Attributes(node: Int, gender: String, nation: String, profession: String)
	
	}
