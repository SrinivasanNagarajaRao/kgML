package net.kugou.algorithms

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * Author: yhao
  * Time: 2018/07/18 11:06
  * Desc: 
  *
  */
object DBSCAN {



  def runDBSCAN(data:Array[Vector[Double]],ePs:Double,minPts:Int):(Array[Int],Array[Int]) ={
    val types = (for(_ <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    val visited = (for(_ <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    val size: Int = data.head.size    //向量长度

    var xTempPoint = new Array[Double](size).toVector
    var yTempPoint = new Array[Double](size).toVector

    var distance = new Array[(Double,Int)](1)
    var distanceTemp = new Array[(Double,Int)](1)
    val neighPoints = new ArrayBuffer[Vector[Double]]()
    var neighPointsTemp = new Array[Vector[Double]](1)
    val cluster = new Array[Int](data.length) //用于标记每个数据点所属的类别

    var index = 0
    for(i <- data.indices){   //对每一个点进行处理
      if(visited(i) == 0){    //表示该点未被处理
        visited(i) == 1       //标记为处理过
        xTempPoint = data(i)  //取到该点
        distance = data.map(x => (vectorDis(x,xTempPoint),data.indexOf(x)))   //取得该点到其他所有点的距离Array{(distance,index)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2))    //找到半径ePs内的所有点(密度相连点集合)

        if(neighPoints.length > 1 && neighPoints.length < minPts){
          breakable{
            for(i <- neighPoints.indices ){   //此为非核心点，若其领域内有核心点，则该点为边界点
            var index = data.indexOf(neighPoints(i))
              if(types(index) == 1){
                types(i) = 0    //边界点
                break
              }
            }
          }
        }

        if(neighPoints.length >= minPts){//核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          types(i) = 1
          cluster(i) = number
          while(neighPoints.nonEmpty){ //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            yTempPoint =neighPoints.head //取集合中第一个点
            index = data.indexOf(yTempPoint)
            if(visited(index) == 0){//若该点未被处理，则标记已处理
              visited(index) = 1
              if(cluster(index)==0) cluster(index) = number //划分到与核心点一样的簇中
              distanceTemp = data.map(x => (vectorDis(x,yTempPoint),data.indexOf(x)))//取得该点到其他所有点的距离Array{(distance,index)}
              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点

              if(neighPointsTemp.length >= minPts) {
                types(index) = 1 //该点为核心点
                for (i <- neighPointsTemp.indices) {
                  //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (cluster(data.indexOf(neighPointsTemp(i))) == 0) {
                    cluster(data.indexOf(neighPointsTemp(i))) = number //只划分簇，没有访问到
                    neighPoints += neighPointsTemp(i)
                  }
                }
              }
              if(neighPointsTemp.length > 1 && neighPointsTemp.length < minPts){//------------new---------------
                breakable{
                  for(i <- neighPointsTemp.indices ){//此为非核心点，若其领域内有核心点，则该点为边界点
                  var index1 = data.indexOf(neighPointsTemp(i))
                    if(types(index1) == 1){
                      types(index) = 0//边界点--------------------------------new---------------------------
                      break
                    }
                  }
                }
              }
            }
            neighPoints-=yTempPoint //将该点剔除
          }//end-while
          number += 1 //进行新的聚类
        }
      }
    }
    (cluster,types)
  }


  def vectorDis(v1: Vector[Double], v2: Vector[Double]):Double = {
    var distance = 0.0
    for(i <- v1.indices){distance += (v1(i) - v2(i)) * (v1(i) - v2(i))}
    math.sqrt(distance)
  }
}
