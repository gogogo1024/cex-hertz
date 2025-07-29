// WebSocket 客户端模块，支持 message_id 去重、消息分发、单播/广播统一处理
// 用法：import wsClient from './lib/websocket';
// wsClient.subscribe('trade', cb)
// wsClient.subscribe('order_update', cb)
// wsClient.connect(url)

interface WSMessage {
  type: string;
  message_id?: string;
  [key: string]: any;
}

type Callback = (msg: WSMessage) => void;

class WSClient {
  private ws: WebSocket | null = null;
  private url: string = '';
  private callbacks: Record<string, Callback[]> = {};
  private messageIdSet: Set<string> = new Set();
  private messageIdQueue: string[] = [];
  private reconnecting = false;

  connect(url: string) {
    this.url = url;
    this.ws = new WebSocket(url);
    this.ws.onmessage = (event) => this.handleMessage(event);
    this.ws.onclose = () => this.handleClose();
    this.ws.onerror = () => this.handleClose();
  }

  private handleMessage(event: MessageEvent) {
    let msg: WSMessage;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return;
    }
    // 优雅的 message_id 去重队列
    if (msg.message_id) {
      if (this.messageIdSet.has(msg.message_id)) return;
      this.messageIdSet.add(msg.message_id);
      this.messageIdQueue.push(msg.message_id);
      if (this.messageIdQueue.length > 1000) {
        const oldId = this.messageIdQueue.shift();
        if (oldId) this.messageIdSet.delete(oldId);
      }
    }
    // 分发到对应 type 的回调
    if (msg.type && this.callbacks[msg.type]) {
      this.callbacks[msg.type].forEach(cb => cb(msg));
    }
  }

  private handleClose() {
    if (!this.reconnecting) {
      this.reconnecting = true;
      setTimeout(() => {
        this.reconnecting = false;
        this.connect(this.url);
      }, 2000);
    }
  }

  subscribe(type: string, cb: Callback) {
    if (!this.callbacks[type]) this.callbacks[type] = [];
    this.callbacks[type].push(cb);
  }

  unsubscribe(type: string, cb: Callback) {
    if (!this.callbacks[type]) return;
    this.callbacks[type] = this.callbacks[type].filter(fn => fn !== cb);
  }

  send(data: any) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }
}

const wsClient = new WSClient();
export default wsClient;
