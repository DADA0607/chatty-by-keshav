Chatty by Keshav - video call fix

Important:
1. Deploy on HTTPS (Render URL is HTTPS).
2. Allow camera and microphone permissions in both browsers.
3. Test from two different accounts/browsers.
4. If calls fail only on different networks, add a TURN server; STUN alone cannot traverse every NAT/firewall.

Render start command:
gunicorn --worker-class eventlet -w 1 app:app
