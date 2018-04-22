import io from 'socket.io-client';
const  socket = io('http://localhost:3001');

function subscribeToQueue(cb) {
  socket.on('tweet_prediction', tweet => cb(null, tweet));
}
export { subscribeToQueue };