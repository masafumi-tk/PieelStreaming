import java.io.{FileInputStream, FileWriter, InputStreamReader}
import java.util.Properties
import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable.HashMap
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.atilika.kuromoji.{Token, Tokenizer}
import com.opencsv.CSVWriter

import scala.collection.mutable


/**
  * Created by AKB428 on 2015/06/05.
  *
  * ref:https://github.com/AKB428/inazuma/blob/master/src/main/scala/inazumaTwitter.scala
  * ref:http://www.intellilink.co.jp/article/column/bigdata-kk01.html
  */
object PieelStreaming {
  /**
    *
    * @param args args(0)=application.properties path (default config/application.properties)
    */

  def main(args: Array[String]): Unit = {
    var configFileName: String = "config/application.properties"
    if (args.length == 1) {
      configFileName = args(0)
    }

    // Load Application Config
    val inStream = new FileInputStream(configFileName)
    val appProperties = new Properties()
    appProperties.load(new InputStreamReader(inStream, "UTF-8"))

    val dictFilePath = appProperties.getProperty("kuromoji.dict_path")
    val takeRankNum = appProperties.getProperty("take_rank_num").toInt

    // TODO @AKB428 サンプルコードがなぜかシステム設定になってるのでプロセス固有に設定できるように直せたら直す
    System.setProperty("twitter4j.oauth.consumerKey", appProperties.getProperty("twitter.consumer_key"))
    System.setProperty("twitter4j.oauth.consumerSecret", appProperties.getProperty("twitter.consumer_secret"))
    System.setProperty("twitter4j.oauth.accessToken", appProperties.getProperty("twitter.access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", appProperties.getProperty("twitter.access_token_secret"))


    // https://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("Mikasa Online Layer")
    conf.setMaster("local[*]")
    //val sc = new SparkContext(conf)

    // Spark Streaming本体（Spark Streaming Context）の定義
    val ssc = new StreamingContext(conf, Seconds(60)) // スライド幅60秒

    // 設定ファイルより検索ワードを設定
    val searchWordList = appProperties.getProperty("twitter.searchKeyword").split(",")

    val stream = TwitterUtils.createStream(ssc, None, searchWordList)   //サーチしたい単語がある時
    //val stream = TwitterUtils.createStream(ssc, None) //ランダムに集計したい時

    // Twitterから取得したツイートを処理する
    val tweetStream = stream.flatMap(status => {

      //val tokenizer : Tokenizer = CustomTwitterTokenizer.builder().build()  // kuromojiの分析器
      val features : scala.collection.mutable.ArrayBuffer[String] = new collection.mutable.ArrayBuffer[String]() //解析結果を保持するための入れ物
      var tweetText : String = status.getText() //ツイート本文の取得
      val japanese_pattern : Pattern = Pattern.compile("[¥¥u3040-¥¥u309F]+") //「ひらがなが含まれているか？」の正規表現
      var feelingscore :Double = 0
      if(japanese_pattern.matcher(tweetText).find()) {  // ひらがなが含まれているツイートのみ処理
        feelingscore = FeelingDictionary.getFellingScore(tweetText) //tweet感情解析
        // 不要な文字列の削除
        tweetText = tweetText.replaceAll("http(s*)://(.*)/", "").replaceAll("¥¥uff57", "") // 全角の「ｗ」は邪魔www

        // ツイート本文の解析
        val tokens : java.util.List[Token] = CustomTwitterTokenizer.tokenize(tweetText, dictFilePath)
        val pattern : Pattern = Pattern.compile("^[a-zA-Z]+$|^[0-9]+$") //「英数字か？」の正規表現
        for(index <- 0 to tokens.size()-1) { //各形態素に対して。。。
        val token = tokens.get(index)

          // 英単語文字を排除したい場合hはこれを使う
          val matcher : Matcher = pattern.matcher(token.getSurfaceForm())

          if(token.getSurfaceForm().length() >= 2 && !matcher.find()) {
            if (tokens.get(index).getAllFeaturesArray()(0) == "名詞" && (tokens.get(index).getAllFeaturesArray()(1) == "一般" || tokens.get(index).getAllFeaturesArray()(1) == "固有名詞")) {
              features += tokens.get(index).getSurfaceForm
              //println(tokens.get(index).getAllFeaturesArray()(1))
            } else if (tokens.get(index).getPartOfSpeech == "カスタム名詞") {
              //println(tokens.get(index).getPartOfSpeech)
              // println(tokens.get(index).getSurfaceForm)
              features += tokens.get(index).getSurfaceForm
              //print(feelCounter.get(tokens.get(index).getSurfaceForm))
            }
          }
        }
      }
      //(feelCounter)
      //単語をキー,感情スコアをvalueにする
      val feelMap = features.map({ tex =>
        (tex,feelingscore)
      })
      feelMap
    })

    // ソート方法を定義（必ずソートする前に定義）
    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.compare(b)*(-1)
    }

    //wordcounterRDD (word,score)の形になっているRDDの各要素にcount用の1を付与し,((word,score),count)の形にする
    //その後RDDの各要素を(word,count)に変更する
    val wordCounter = tweetStream.map((_,1)).map{case ((word,score),count) => (word,count)}
    val wordCounts60 = wordCounter.reduceByKeyAndWindow(_+_,Seconds(5*60))
        .map{case (word,count) => (count,word)}.transform(_.sortByKey(true))
    //feelCounts 書くwordのfeelingScoreを集計
    val feelCounts60 = tweetStream.reduceByKeyAndWindow(_+_, Seconds(5*60))
        .map{case (topic, count) => (count, topic)}
        .transform(_.sortByKey(true))

    val wordAndFeelingCounter = wordCounts60.map{case (count,word) => (word,count)}
        .join(feelCounts60.map{case (score,word2) => (word2,score)})
        .map{case (word,(count,score)) => (count,(word,score))}.transform(_.sortByKey(true))

    // ウインドウ集計

    /**
      * val topCountsFeeling = tweetStream.map((_, 1)                      // 出現回数をカウントするために各単語に「1」を付与
      * ).reduceByKeyAndWindow(_+_, Seconds(5*60)   // ウインドウ幅(60*60sec)に含まれる単語を集める
      * ).map{case (topic, count) => (count, topic)  // 単語の出現回数を集計
      * }.transform(_.sortByKey(true))               // ソート
      */

    // 出力
    wordAndFeelingCounter.foreachRDD(rdd => {
      // 出現回数上位(takeRankNum)個の単語を取得
      val sendMsg = new StringBuilder()
      val topList = rdd.take(takeRankNum)
      // コマンドラインに出力
      println("¥ nPopular topics in last 5*60 seconds (%s words):".format(rdd.count()))
      var countRank : Int = 1;
      topList.foreach{case (count,(word,score)) =>
        println("%d位:word:%s,count:%d,score:%f".format(countRank,word,count,score/count))
        sendMsg.append("word:%s,count:%d,score:%f,".format(word,count,score/count))
        countRank += 1
      }
      println(sendMsg.toString())
    })

    ssc.start()
    ssc.awaitTermination()
  }

}

object CustomTwitterTokenizer{
  def tokenize(text: String, dictPath: String): java.util.List[Token] = {
    Tokenizer.builder().mode(Tokenizer.Mode.SEARCH)
      .userDictionary(dictPath)
      .build().tokenize(text)
  }
}
//感情解析
object FeelingDictionary{
  val feelMap = new HashMap[String,Double];
  setFeelMap() //感情スコアマップの生成

  def getFellingScore(str:String) : Double = {
    val tokenizer:Tokenizer = Tokenizer.builder().mode(Tokenizer.Mode.NORMAL).build()
    val tokens = tokenizer.tokenize(str).toArray()
    var tokenScore : Double = 0
    var allScore : Double = 0;
    var count:Int = 0

    tokens.foreach({ t =>
      val token = t.asInstanceOf[Token]
      if(feelMap.contains(token.getSurfaceForm())==true){
        tokenScore = feelMap(token.getSurfaceForm())
        allScore += tokenScore
        count += 1
      }
    })
    return allScore/count
  }

  def setFeelMap(){
    var word : Array[String] = null
    val source = scala.io.Source.fromFile("dictionary/FeelingScoreJP.txt")
    source.getLines.foreach({line =>
      word = line.split(":",0)
      feelMap.put(word(0),word(3).toDouble)
    })
    source.close
  }
}
object Csvperser{
  def opencsvToStringArray(args:Array[String]): Unit ={

  }
  def opencsvToBean(): Unit ={

  }
  def readFromCsvFile(): Unit ={

  }
  def writeToCsvFileGeo(args: Array[String]): Unit ={

    var writer = new CSVWriter(new FileWriter("output/outPutGeo.csv",true))
    var strArr = args
    writer.writeNext(strArr)
    writer.flush()
  }
  def writeToCsvFile(args: Array[String]): Unit ={

    var writer = new CSVWriter(new FileWriter("output/outPut.csv",true))
    var strArr = args
    writer.writeNext(strArr)
    writer.flush()
  }
  //  def isFileValid(): Unit ={
  //
  //  }
}
