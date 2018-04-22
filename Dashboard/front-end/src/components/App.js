import React, { Component } from 'react';
import { subscribeToQueue } from './../api/TweetsAPI';
import {Bar, Line, Pie} from 'react-chartjs-2';

class App extends Component {

    state = {
        tweet_pred: []
    };

    constructor(props) {
        super(props);
        subscribeToQueue((err, tweet) => {
            var tweets = this.state.tweet_pred;
            console.log(tweet);
            tweets.push(JSON.parse(tweet));   
            this.setState({
                tweet_pred : tweets
        })});
    }

    createItemsList(){
        return this.state.tweet_pred.map((tweet) => {
            return(
                    <tr>
                        <td>
                            {tweet.text}
                        </td>
                        <td width = "40">
                                +ve
                        </td>
                    </tr>
            );
        });
    }

    render() {
        return (
        <div class="container-fluid">
            <div class="row">
                <nav class="col-md-2 d-none d-md-block bg-light sidebar">
                    <div class="sidebar-sticky">
                        <ul class="nav flex-column">
                            <li class="nav-item">
                                <a class="nav-link active">
                                    Sentiment Analysis
                                    <span class="sr-only">(current)</span>
                                </a>
                            </li>
                            <li class="nav-item">
                                <input className="form-control" type="text" label="Keyword" placeholder="Keyword"/>
                            </li>
                            <li class="nav-item">
                                <div class="btn-toolbar mb-2 mb-md-0">
                                    <button class="btn btn-sm btn-outline-secondary">Analyze Tweets</button>
                                </div>
                            </li>
                        </ul>
                    </div>
                </nav>

                <main role="main" class="col-md-9 ml-sm-auto col-lg-10 px-4">
                    <div class="d-flex justify-content-between flex-wrap flex-md-nowrap align-items-center pt-3 pb-2 mb-3 border-bottom">
                        <h1 class="h2">Dashboard</h1>
                    </div>

                    <canvas class="my-4 w-100" id="myChart" width="900" height="380"></canvas>

                    <h2>Tweet Sentiments</h2>
                    <div class="table-responsive">
                        <table class="table table-striped table-sm">
                            <thead>
                                <tr>
                                    <th>Tweet</th>
                                    <th width = "40">Sentiment</th>
                                </tr>
                            </thead>
                            <tbody>
                                {this.createItemsList()}
                            </tbody>
                        </table>
                    </div>
                </main>
            </div>
        </div>
        );
    }
}

export default App;
