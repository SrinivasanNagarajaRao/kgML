
package net.kugou.pipeline

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.DataType

/**
 * Created by chaoliu on 2017/10/25.
 */
class KgTransformer[I, O](override val uid: String) extends UnaryTransformer[I, O, KgTransformer[I, O]] with DefaultParamsWritable {

    var kgOutputDataType: DataType = _
    var f: I => O = _

    def this(kgOutputDataType: DataType, f: I => O) = {
        this(Identifiable.randomUID("kgTransformer"))
        this.kgOutputDataType = kgOutputDataType
        this.f = f
    }

    override def outputDataType: DataType = kgOutputDataType

    //自定义实现验证
    override def validateInputType(inputType: DataType): Unit = {}

    override def createTransformFunc: (I) => O = {

        // 这里要返回一个 类型为I，输出类型为O的函数
        //其实这玩意就是想搞一个map的函数体来
        (in: I) => f(in)
    }
}
