# Twitter-Sentiment-Analysis
The project is to analyze sentiment among users for any event. Project involves analyzing the project in a batch mode and representing it to end users. Also, we will use real time stream data and analyze it in motion. After analysis, the result would be presented as the streaming is taking place.

## Architecture

![Alt text](Architecture_Diagram.png?raw=true "Architecture Diagram")

## Flow

1. Twitter streaming API will give the live data about the tweets which will be input for spark streaming
2. Spark streaming will process the stream and will give batches of input data to one of the node in HDFS cluster.
3. Node on the cluster will use trained Machine Learning model to predict the sentiment of the input tweets with the help of Spark MLlib and will give its feedback to message broker.
4. Dashboard will continuously listen to message broker for the new tweet data and will display whenever the data is available.


## Technologies:

1. **Python3 : pandas, numpy** with Jupyter Notebook (Data Cleaning)
2. **Spark MLLib: pyspark** (Model Training Library to be used for Sentiment Analysis)
3. **Twitter Streaming API** (getting live twitter data)
4. **Spark Streaming** (handles stream of input data for processing)
5. **Redis Queue** (Message Broker)
6. **NodeJS Application** (Dashboard)


## Datasets:
    
* Training Data:
    http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip
    
* Test Data:
    Live twitter data.


## Model Training

We are trying to implement the two different machine learning algorithms and will lookout for the model which produces the best accuracy and classifies the tweet sentiment correctly. Machine Learning algorithms to be implemented are as below:

    1. CountVectorizer + Logistic Regression Modeling
    2. Multi-Layer Perceptron Modelling (Neural Network Modelling)
