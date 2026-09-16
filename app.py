from flask import Flask, render_template, request, send_file, abort, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from datetime import datetime, timezone, timedelta
import uuid, secrets, string, os, mimetypes, json, sqlite3, threading

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024 * 1024 + 1024 * 1024
MEDIA_DIR = os.path.join(os.path.dirname(__file__), 'media')
os.makedirs(MEDIA_DIR, exist_ok=True)
app.config['SECRET_KEY'] = 'chatty-by-keshav'
socketio = SocketIO(app, cors_allowed_origins='*', max_http_buffer_size=50*1024*1024*1024, async_mode='threading')

users = {}          # sid -> user
user_index = {}     # stable user_id -> latest sid
groups = {}         # code -> group metadata
statuses = {}       # stable user_id -> status
profiles = {}       # stable user_id -> persistent public profile metadata
messages = {}       # room -> {id: message}
DB_FILE = os.path.join(os.path.dirname(__file__), 'chatty.sqlite3')
_db_lock = threading.RLock()

def timestamp(): return datetime.now(timezone.utc).isoformat()

def db_conn():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db_conn() as db:
        db.execute('CREATE TABLE IF NOT EXISTS profiles (user_id TEXT PRIMARY KEY, name TEXT NOT NULL, avatar TEXT NOT NULL, last_seen TEXT, created_at TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, room TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS groups_store (code TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS statuses_store (user_id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS idx_messages_room ON messages(room, created_at)')
        for r in db.execute('SELECT user_id,name,avatar,last_seen FROM profiles'):
            profiles[r['user_id']] = {'name':r['name'], 'avatar':r['avatar'], 'last_seen':r['last_seen']}
        for r in db.execute('SELECT room,id,payload FROM messages ORDER BY created_at'):
            try: messages.setdefault(r['room'], {})[r['id']] = json.loads(r['payload'])
            except Exception: pass
        for r in db.execute('SELECT code,payload FROM groups_store'):
            try:
                g=json.loads(r['payload']); g['members']=set(g.get('members',[])); groups[r['code']]=g
            except Exception: pass
        for r in db.execute('SELECT user_id,payload FROM statuses_store'):
            try: statuses[r['user_id']]=json.loads(r['payload'])
            except Exception: pass

def save_profile(uid):
    p=profiles.get(uid)
    if not p: return
    with _db_lock, db_conn() as db:
        db.execute('INSERT INTO profiles(user_id,name,avatar,last_seen,created_at) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET name=excluded.name,avatar=excluded.avatar,last_seen=excluded.last_seen', (uid,p.get('name','User'),p.get('avatar','/static/avatars/avatar1.svg'),p.get('last_seen'),timestamp()))

def save_message(msg):
    with _db_lock, db_conn() as db:
        db.execute('INSERT OR REPLACE INTO messages(id,room,payload,created_at) VALUES(?,?,?,?)', (msg['id'],msg['room'],json.dumps(msg,ensure_ascii=False),msg.get('time',timestamp())))

def delete_db_message(mid):
    with _db_lock, db_conn() as db: db.execute('DELETE FROM messages WHERE id=?',(mid,))

def save_group(code):
    g=groups.get(code)
    if not g: return
    payload=dict(g); payload['members']=list(g.get('members',set()))
    with _db_lock, db_conn() as db:
        db.execute('INSERT OR REPLACE INTO groups_store(code,payload) VALUES(?,?)',(code,json.dumps(payload,ensure_ascii=False)))

def save_status(uid):
    st=statuses.get(uid)
    with _db_lock, db_conn() as db:
        if st is None: db.execute('DELETE FROM statuses_store WHERE user_id=?',(uid,))
        else: db.execute('INSERT OR REPLACE INTO statuses_store(user_id,payload) VALUES(?,?)',(uid,json.dumps(st,ensure_ascii=False)))

init_db()

def public_user(uid):
    p=profiles.get(uid, {})
    sid=user_index.get(uid)
    u=users.get(sid) if sid else None
    if u:
        return {'id':uid,'name':u['name'],'avatar':u['avatar'],'online':True,'last_seen':None,'status':statuses.get(uid,{}).get('text','')}
    return {'id':uid,'name':p.get('name','User'),'avatar':p.get('avatar','/static/avatars/avatar1.svg'),'online':False,'last_seen':p.get('last_seen'),'status':statuses.get(uid,{}).get('text','')}

def room_users(room):
    out=[]
    for u in users.values():
        if u.get('room') == room:
            out.append({'id':u['id'],'name':u['name'],'avatar':u['avatar'],'online':True,'status':statuses.get(u['id'],{}).get('text','')})
    return out

def online_users():
    ids = list(profiles.keys())
    for u in users.values():
        if u['id'] not in ids: ids.append(u['id'])
    return [public_user(uid) for uid in ids]

def clean_statuses():
    cutoff=datetime.now(timezone.utc)-timedelta(hours=24)
    for uid in list(statuses):
        try:
            if datetime.fromisoformat(statuses[uid]['time']) < cutoff: statuses.pop(uid,None)
        except Exception: statuses.pop(uid,None)

def public_statuses():
    clean_statuses()
    out=[]
    for uid, st in statuses.items():
        sid=user_index.get(uid); u=users.get(sid)
        if u:
            out.append({'id':uid,'name':u['name'],'avatar':u['avatar'],**st})
    return out

def send_statuses(target=None):
    socketio.emit('statuses_list', public_statuses(), to=target) if target else socketio.emit('statuses_list', public_statuses())

def clean_statuses():
    cutoff=datetime.now(timezone.utc)-timedelta(hours=24)
    for uid in list(statuses):
        try:
            if datetime.fromisoformat(statuses[uid]['time']) < cutoff: statuses.pop(uid,None)
        except Exception: statuses.pop(uid,None)

def public_statuses():
    clean_statuses()
    out=[]
    for uid, st in statuses.items():
        sid=user_index.get(uid); u=users.get(sid)
        if u: out.append({'id':uid,'name':u['name'],'avatar':u['avatar'],**st})
    return out

def send_statuses(target=None):
    if target: socketio.emit('statuses_list', public_statuses(), to=target)
    else: socketio.emit('statuses_list', public_statuses())

def send_user_list(room=None):
    # Personal-chat contacts must remain available even while the user is viewing a group/DM.
    socketio.emit('user_list', online_users())

def my_groups(user_id):
    # Never expose invite codes in the normal group list. The creator gets the code only
    # through the dedicated creator_invite event when opening their group.
    return [{'name':g['name'], 'room':g['room'], 'avatar':g.get('avatar','/static/avatars/avatar1.svg'), 'is_creator': user_id == g['creator_id']} for code,g in groups.items() if user_id in g['members']]

def send_groups(sid):
    u=users.get(sid)
    if u: emit('groups_list', my_groups(u['id']), to=sid)

def leave_current_room(sid):
    u=users.get(sid)
    if not u: return
    old=u.get('room')
    if old:
        leave_room(old, sid=sid)
        send_user_list(old)

@app.route('/')
def home(): return render_template('index.html')

def file_limit(filename, mime):
    name = (filename or '').lower()
    if mime.startswith('image/'):
        return 15 * 1024 * 1024, '15 MB'
    if mime.startswith('video/'):
        return 10 * 1024 * 1024 * 1024, '10 GB'
    if name.endswith(('.zip', '.rar')):
        return 50 * 1024 * 1024 * 1024, '50 GB'
    return 50 * 1024 * 1024, '50 MB'

def room_authorized(uid, room):
    if room == 'global': return True
    if room.startswith('group_'):
        return any(g['room'] == room and uid in g['members'] for g in groups.values())
    if room.startswith('dm_'):
        parts = room[3:].split('_')
        return len(parts) == 2 and uid in parts
    return False

@app.route('/upload', methods=['POST'])
def upload_media():
    uid = str(request.form.get('user_id', '')).strip()
    room = str(request.form.get('room', '')).strip()
    sid = user_index.get(uid)
    u = users.get(sid) if sid else None
    f = request.files.get('file')
    if not u or not f or not room_authorized(uid, room):
        return jsonify({'error':'Upload is not authorized.'}), 403
    filename = os.path.basename(f.filename or 'file')[:160]
    mime = (f.mimetype or mimetypes.guess_type(filename)[0] or 'application/octet-stream').lower()[:100]
    limit, label = file_limit(filename, mime)
    # Stream into Werkzeug's temporary file without reading it into Python memory.
    temp_name = uuid.uuid4().hex + '.upload'
    temp_path = os.path.join(MEDIA_DIR, temp_name)
    total = 0
    try:
        with open(temp_path, 'wb') as out:
            while True:
                chunk = f.stream.read(1024 * 1024)
                if not chunk: break
                total += len(chunk)
                if total > limit:
                    raise ValueError(label)
                out.write(chunk)
    except ValueError as e:
        try: os.remove(temp_path)
        except OSError: pass
        return jsonify({'error':f'File is too large. Maximum allowed for this type is {e.args[0]}.'}), 413
    except Exception:
        try: os.remove(temp_path)
        except OSError: pass
        return jsonify({'error':'Upload failed.'}), 500
    media_id = uuid.uuid4().hex
    final_name = media_id + os.path.splitext(filename)[1].lower()
    final_path = os.path.join(MEDIA_DIR, final_name)
    os.replace(temp_path, final_path)
    msg = {'id':media_id,'name':u['name'],'sender_id':uid,'avatar':u['avatar'],'message':str(request.form.get('caption',''))[:500],
           'time':timestamp(),'reply':None,'type':'media','room':room,'filename':filename,'mime':mime,
           'url':'/media/'+final_name,'size':total}
    messages.setdefault(room,{})[msg['id']] = msg
    socketio.emit('message', msg, to=room)
    return jsonify({'ok':True,'message':msg})

@app.route('/media/<path:name>')
def stream_media(name):
    safe = os.path.basename(name)
    path = os.path.join(MEDIA_DIR, safe)
    if not os.path.isfile(path): abort(404)
    mime = mimetypes.guess_type(path)[0] or 'application/octet-stream'
    # Flask's conditional send_file supports HTTP Range requests, allowing browser
    # video players to seek/stream without downloading the entire file first.
    return send_file(path, mimetype=mime, conditional=True, max_age=0)

@socketio.on('join')
def join_chat(data):
    uid=str(data.get('user_id','')).strip()[:80] or uuid.uuid4().hex
    saved=profiles.get(uid, {})
    name=str(data.get('name','')).strip()[:30] or saved.get('name','')
    avatar=str(data.get('avatar','')).strip() or saved.get('avatar','/static/avatars/avatar1.svg')
    allowed={f'/static/avatars/avatar{i}.svg' for i in range(1,9)}
    if not (avatar.startswith('data:image/') or avatar in allowed): avatar='/static/avatars/avatar1.svg'
    if not name: return
    old=users.get(request.sid)
    if old: leave_current_room(request.sid)
    # kick previous socket for same stable user id from its old room
    prev_sid=user_index.get(uid)
    if prev_sid and prev_sid != request.sid and prev_sid in users:
        leave_current_room(prev_sid)
        users.pop(prev_sid, None)
    room='global'
    users[request.sid]={'id':uid,'name':name,'avatar':avatar,'room':room}
    profiles[uid]={'name':name,'avatar':avatar,'last_seen':None}
    save_profile(uid)
    user_index[uid]=request.sid
    join_room(room)
    messages.setdefault(room,{})
    emit('joined_state', {'user_id':uid, 'room':room})
    send_statuses(request.sid)
    emit('history', list(messages[room].values())[-100:])
    emit('system', {'message':f'{name} joined the conversation.','time':timestamp()}, to=room)
    send_user_list(room); send_groups(request.sid)

@socketio.on('update_profile')
def update_profile(data):
    u=users.get(request.sid)
    if not u: return
    avatar=str(data.get('avatar',''))
    allowed={f'/static/avatars/avatar{i}.svg' for i in range(1,9)}
    if not (avatar.startswith('data:image/') or avatar in allowed): return
    u['avatar']=avatar
    profiles.setdefault(u['id'],{})['name']=u['name']; profiles[u['id']]['avatar']=avatar
    save_profile(u['id'])
    socketio.emit('profile_updated', {'id':u['id'],'name':u['name'],'avatar':avatar}, to=u['room'])
    send_user_list(u['room'])

@socketio.on('set_status')
def set_status(data):
    u=users.get(request.sid)
    if not u:return
    text=str(data.get('text','')).strip()[:120]
    media=str(data.get('media',''))
    mime=str(data.get('mime',''))[:100]
    filename=str(data.get('filename',''))[:120]
    if len(media)>8_500_000: emit('error_message',{'message':'Status media is too large. Maximum is about 6 MB.'}); return
    if media and not (mime.startswith('image/') or mime.startswith('video/')):
        emit('error_message',{'message':'Status media must be an image or video.'}); return
    statuses[u['id']]={'text':text,'time':timestamp(),'media':media,'mime':mime,'filename':filename}
    save_status(u['id'])
    emit('my_status', statuses[u['id']], to=request.sid)
    socketio.emit('status_updated', {'id':u['id'],'name':u['name'],'avatar':u['avatar'],'text':text,'media':media,'mime':mime,'filename':filename}); send_statuses(); send_user_list(u['room'])

@socketio.on('get_statuses')
def get_statuses():
    send_statuses(request.sid)

@socketio.on('get_status')
def get_status(data):
    uid=str(data.get('user_id',''))
    s=statuses.get(uid)
    if s and datetime.fromisoformat(s['time']) < datetime.now(timezone.utc)-timedelta(hours=24):
        statuses.pop(uid,None); save_status(uid); s=None
    emit('status_result', {'user_id':uid,'status':s}, to=request.sid)

@socketio.on('create_group')
def create_group(data):
    u=users.get(request.sid)
    if not u:return
    name=str(data.get('name','')).strip()[:40]
    if not name: emit('error_message',{'message':'Please enter a group name.'}); return
    alphabet=string.ascii_uppercase+string.digits
    code=''.join(secrets.choice(alphabet) for _ in range(6))
    while code in groups: code=''.join(secrets.choice(alphabet) for _ in range(6))
    room='group_'+code
    groups[code]={'name':name,'room':room,'creator_id':u['id'],'creator_name':u['name'],'members':{u['id']},'avatar':'/static/avatars/avatar1.svg'}
    save_group(code)
    messages.setdefault(room,{})
    old=u['room']; leave_current_room(request.sid)
    u['room']=room; join_room(room)
    emit('group_created', {'name':name,'code':code,'room':room,'creator':u['name'],'creator_id':u['id'],'invite_code':code})
    emit('history', list(messages[room].values())[-100:])
    emit('system', {'message':f'You created {name}.','time':timestamp()}, to=room)
    send_user_list(old); send_user_list(room); send_groups(request.sid)

@socketio.on('join_group')
def join_group(data):
    u=users.get(request.sid)
    if not u:return
    code=str(data.get('code','')).strip().upper()
    g=groups.get(code)
    if not g: emit('error_message',{'message':'Invalid group code. Check the code and try again.'}); return
    g['members'].add(u['id'])
    save_group(code)
    old=u['room']; leave_current_room(request.sid)
    u['room']=g['room']; join_room(g['room']); messages.setdefault(g['room'],{})
    emit('group_joined', {'name':g['name'],'code':code,'room':g['room'],'avatar':g.get('avatar','/static/avatars/avatar1.svg')})
    emit('history', list(messages[g['room']].values())[-100:])
    emit('system', {'message':f"{u['name']} joined {g['name']}.",'time':timestamp()}, to=g['room'])
    send_user_list(old); send_user_list(g['room']); send_groups(request.sid)

@socketio.on('open_group_room')
def open_group_room(data):
    u=users.get(request.sid); room=str(data.get('room','')).strip()
    if not u or not room: return
    match=None; code=None
    for c,g in groups.items():
        if g['room']==room:
            match=g; code=c; break
    if not match or u['id'] not in match['members']:
        emit('error_message',{'message':'You are not a member of this group.'}); return
    old=u['room']; leave_current_room(request.sid); u['room']=room; join_room(room)
    emit('history',list(messages.get(room,{}).values())[-100:]); emit('room_opened',{'name':match['name'],'room':room,'group':True,'avatar':match.get('avatar','/static/avatars/avatar1.svg'),'creator_id':match['creator_id']})
    send_user_list(old); send_user_list(room)
    if u['id']==match['creator_id']:
        emit('creator_invite',{'name':match['name'],'code':code})

@socketio.on('open_group')
def open_group(data):
    u=users.get(request.sid); code=str(data.get('code','')).strip().upper()
    if not u or code not in groups or u['id'] not in groups[code]['members']:
        emit('error_message',{'message':'You are not a member of this group.'}); return
    g=groups[code]; old=u['room']; leave_current_room(request.sid); u['room']=g['room']; join_room(g['room'])
    emit('history',list(messages.get(g['room'],{}).values())[-100:]); emit('room_opened',{'name':g['name'],'room':g['room'],'group':True,'avatar':g.get('avatar','/static/avatars/avatar1.svg'),'creator_id':g['creator_id']})
    send_user_list(old); send_user_list(g['room'])
    # invite code is returned ONLY to creator
    if u['id']==g['creator_id']: emit('creator_invite',{'name':g['name'],'code':code})

@socketio.on('dm')
def direct_message(data):
    u=users.get(request.sid)
    target=str(data.get('target_id','')).strip()
    text=str(data.get('message','')).strip()[:2000]
    if not u or not target or not text or target==u['id']: return
    target_sid=user_index.get(target)
    target_user=users.get(target_sid) if target_sid else None
    room='dm_'+('_'.join(sorted([u['id'],target])))
    # ensure sender/recipient can receive this room
    join_room(room, sid=request.sid)
    if target_sid and target_sid in users: join_room(room, sid=target_sid)
    msg={'id':uuid.uuid4().hex,'name':u['name'],'sender_id':u['id'],'target_id':target,'avatar':u['avatar'],'message':text,'time':timestamp(),'reply':None,'type':'text','dm':True,'room':room}
    messages.setdefault(room,{})[msg['id']]=msg
    save_message(msg)
    socketio.emit('message',msg,to=room)

@socketio.on('open_dm')
def open_dm(data):
    u=users.get(request.sid); target=str(data.get('target_id','')).strip()
    if not u or not target or target==u['id']: return
    target_sid=user_index.get(target)
    room='dm_'+('_'.join(sorted([u['id'],target])))
    old=u['room']; leave_current_room(request.sid); u['room']=room; join_room(room)
    if target_sid and target_sid in users: join_room(room,sid=target_sid)
    messages.setdefault(room,{})
    emit('history',list(messages[room].values())[-100:]); emit('room_opened',{'name':target_user_name(target),'room':room,'dm_target':target,'avatar':target_user(target).get('avatar'),'online':target_user(target).get('online'),'last_seen':target_user(target).get('last_seen')})
    send_user_list(old)

def target_user(uid):
    return public_user(uid)

def target_user_name(uid):
    return target_user(uid)['name']

@socketio.on('get_user_profile')
def get_user_profile(data):
    uid=str(data.get('user_id','')).strip()
    if uid: emit('user_profile', target_user(uid), to=request.sid)

@socketio.on('update_group_profile')
def update_group_profile(data):
    u=users.get(request.sid)
    room=str(data.get('room','')).strip()
    avatar=str(data.get('avatar',''))
    if not u or not room or not (avatar.startswith('data:image/') or avatar.startswith('/static/avatars/')): return
    match=next((g for g in groups.values() if g['room']==room),None)
    if not match or match['creator_id']!=u['id']: emit('error_message',{'message':'Only the group creator can change the group photo.'}); return
    if avatar.startswith('data:image/') and len(avatar)>3_000_000: emit('error_message',{'message':'Group photo is too large. Maximum is about 2 MB.'}); return
    match['avatar']=avatar
    save_group(next(c for c,g in groups.items() if g is match))
    payload={'room':room,'name':match['name'],'avatar':avatar}
    socketio.emit('group_profile_updated',payload,to=room)
    send_groups(request.sid)

@socketio.on('message')
def new_message(data):
    u=users.get(request.sid)
    if not u:return
    text=str(data.get('message','')).strip()[:2000]
    if not text:return
    reply=data.get('reply') if isinstance(data.get('reply'),dict) else None
    msg={'id':uuid.uuid4().hex,'name':u['name'],'sender_id':u['id'],'avatar':u['avatar'],'message':text,'time':timestamp(),'reply':reply,'type':'text','room':u['room']}
    messages.setdefault(u['room'],{})[msg['id']]=msg
    save_message(msg)
    emit('message',msg,to=u['room'])

# Large media is uploaded through /upload so video can be streamed with HTTP Range requests.

@socketio.on('delete_message')
def delete_message(data):
    u=users.get(request.sid)
    if not u:return
    mid=str(data.get('id','')); room=u['room']; msg=messages.get(room,{}).get(mid)
    if msg and msg.get('sender_id')==u['id']:
        del messages[room][mid]; delete_db_message(mid); emit('message_deleted',{'id':mid},to=room)

@socketio.on('typing')
def typing(data):
    u=users.get(request.sid)
    if u: emit('typing',{'name':u['name'],'typing':bool(data.get('typing'))},to=u['room'],include_self=False)


@socketio.on('call_offer')
def call_offer(data):
    u=users.get(request.sid); target=str(data.get('to','')).strip()
    target_sid=user_index.get(target)
    if not u or not target_sid or target_sid not in users:
        emit('call_unavailable'); return
    emit('call_offer', {'from':u['id'],'name':u['name'],'avatar':u['avatar'],'kind':str(data.get('kind','audio')),'offer':data.get('offer')}, to=target_sid)

@socketio.on('call_answer')
def call_answer(data):
    u=users.get(request.sid); target=str(data.get('to','')).strip(); target_sid=user_index.get(target)
    if u and target_sid: emit('call_answer', {'from':u['id'],'answer':data.get('answer')}, to=target_sid)

@socketio.on('call_ice')
def call_ice(data):
    u=users.get(request.sid); target=str(data.get('to','')).strip(); target_sid=user_index.get(target)
    if u and target_sid: emit('call_ice', {'from':u['id'],'candidate':data.get('candidate')}, to=target_sid)

@socketio.on('call_reject')
def call_reject(data):
    u=users.get(request.sid); target_sid=user_index.get(str(data.get('to','')).strip())
    if u and target_sid: emit('call_reject', {'from':u['id']}, to=target_sid)

@socketio.on('call_end')
def call_end(data):
    u=users.get(request.sid); target_sid=user_index.get(str(data.get('to','')).strip())
    if u and target_sid: emit('call_end', {'from':u['id']}, to=target_sid)

@socketio.on('disconnect')
def disconnect():
    u=users.pop(request.sid,None)
    if u:
        if user_index.get(u['id'])==request.sid:
            user_index.pop(u['id'],None)
            profiles.setdefault(u['id'],{})['name']=u['name']; profiles[u['id']]['avatar']=u['avatar']; profiles[u['id']]['last_seen']=timestamp(); save_profile(u['id'])
        emit('system',{'message':f"{u['name']} left the conversation.",'time':timestamp()},to=u.get('room'))
        send_user_list(u.get('room'))

if __name__=='__main__':
    print('\n====================================\n   Chatty by Keshav is running\n   http://127.0.0.1:5000\n====================================\n')
    socketio.run(app,host='0.0.0.0',port=5000,debug=False,allow_unsafe_werkzeug=True)
