package be.kdg;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;
import org.apache.spark.streaming.twitter.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class StartMain {

    public static void main(String[] args) {

        System.out.println("Test");

        if (args.length < 4) {
            System.err.println("Usage: JavaTwitterHashTagJoinSentiments <consumer key>" +
                    " <consumer secret> <access token> <access token secret> [<filters>]");
            System.exit(1);
        }
        /*//Zaken aanmaken via een twitterapp (Bron: youtube video https://www.youtube.com/watch?v=1GixYso8Az4&t=577s)
        final String consumerKey = "KZSQvBjJpVBWvb97QHeL2pbYV";
        final String consumerSecret = "6Auksnx9RNxON8XXldeiDnrTSgJOoGYjSW7R56sdyjRnJ8AotA";
        final String accessToken = "948994158479409159-zgeiikgTqDbwbZ7s7iv5otmjeyZJ20F";
        final String accessTokenSecret = "VluOhqkwvFUEpp7rwPehDV7IqWOdzETJ4wtxki7GZugqi";

        String[] filters = {"#NowPlaying", "#nowplaying"};
*/

        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];
        String[] filters = {"#NowPlaying"};

        //Systeemproperties instellen zodat twitterstream gebruikt kan worden
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        //Sparkconfiguratie opstarten
        //Local 2: we gaan local werken?
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("MusicAnalyser");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));


        //Stream opstarten met de juiste filters
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, filters);

        //Woorden er uit halen en deze in een array opslaan?
        JavaDStream<String> words = twitterStream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterator<String> call(Status s) {
                return Arrays.asList(s.getText().split(" ")).iterator();
            }
        });

        words.dstream().saveAsTextFiles("file:///C:\\Users\\Fran\\Dropbox\\Toegepase informatica\\MD ETL BIGDATA NOSQL\\Examenproject\\testbestand", "txt\"");


        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}