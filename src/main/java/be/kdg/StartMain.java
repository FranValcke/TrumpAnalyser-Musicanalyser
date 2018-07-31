package be.kdg;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class StartMain {

    public static void main(String[] args) {
        final String consumerKey = "KZSQvBjJpVBWvb97QHeL2pbYV";
        final String consumerSecret = "6Auksnx9RNxON8XXldeiDnrTSgJOoGYjSW7R56sdyjRnJ8AotA";
        final String accessToken = "948994158479409159-zgeiikgTqDbwbZ7s7iv5otmjeyZJ20F";
        final String accessTokenSecret = "VluOhqkwvFUEpp7rwPehDV7IqWOdzETJ4wtxki7GZugqi";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("MusicAnalyzer");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);

        String[] filters = {"#NowPlaying", "#nowplaying"};

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc, twitterAuth, filters);

        JavaDStream<String> statusses = twitterStream.map(
                new Function<Status, String>() {
                    @Override
                    public String call(Status status) {
                        StringBuilder sb = new StringBuilder();

                        String text = status.getText();
                        String[] strArray = text.split(" ");


                        //Patroon 1: #Nowplaying ... by ...
                        if (strArray[0].startsWith("#")) {
                            int teller = 0;
                            int positie = 0;
                            for (String s : strArray) {
                                if (s.startsWith("by") || s.startsWith("-") || s.startsWith("door")) {
                                    positie = teller;
                                    break;
                                }
                                teller++;
                            }
                            // artist toevoegen
                            sb.append(strArray[teller + 1]);
                            sb.append("\n");

                            //String maken van de track
                            StringBuilder title = new StringBuilder();
                            for (int i = 1; i < positie; i++) {
                                title = title.append(strArray[i]);
                            }
                            //track title toevoegen
                            sb.append(title);
                            sb.append("\n");
                        }

                        sb.append(status.getText());

                        //artist
                        // sb.append(strArray[artist]);
                        sb.append("\n");
                        //Tracktitle

                        sb.append("\n");
                        //Overigetekst

                        //Taal
                        sb.append(status.getLang());
                        sb.append("\n");
                        //user
                        sb.append(status.getUser().getName());
                        sb.append("\n");
                        //timestamp
                        sb.append(status.getCreatedAt());
                        sb.append("\n");
                        //TweetId
                        sb.append(status.getId());
                        sb.append("\n");
                        //Hashtags
                        String text1 = status.getText();
                        String[] strArray1 = text1.split(" ");
                        for (String s : strArray1) {
                            if (s.startsWith("#")) {
                                sb.append(s);
                            }
                        }


                        return sb.toString();
                    }
                }
        );

        statusses.dstream().saveAsTextFiles("file:///C:/BigDataStreaming/MusicStream", "txt");

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}


