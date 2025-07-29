import React, { useState, useEffect } from 'react';
import { create } from 'zustand';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import wsClient from './lib/websocket';

// 成交类型
export interface Trade {
  id: string;
  price: number;
  amount: number;
  side: string;
  ts: string;
  symbol: string; // 新增 symbol 字段
}

// Zustand store
interface TradeStore {
  trades: Trade[];
  addTrade: (trade: Trade) => void;
  setTrades: (trades: Trade[]) => void;
}

export const useTradeStore = create<TradeStore>((set) => ({
  trades: [],
  addTrade: (trade) => set((state) => ({ trades: [trade, ...state.trades].slice(0, 50) })),
  setTrades: (trades) => set(() => ({ trades: trades.slice(0, 50) })),
}));

// 订单状态变更消息类型
interface OrderUpdateMsg {
  order_id: string;
  status: string;
  [key: string]: any;
}

interface OrderUpdateStore {
  orderUpdates: OrderUpdateMsg[];
  addOrderUpdate: (msg: OrderUpdateMsg) => void;
  removeOrderUpdate: (order_id: string) => void;
}

export const useOrderUpdateStore = create<OrderUpdateStore>((set) => ({
  orderUpdates: [],
  addOrderUpdate: (msg) => set((state) => ({
    orderUpdates: [msg, ...state.orderUpdates].slice(0, 5) // 最多保留5条
  })),
  removeOrderUpdate: (order_id) => set((state) => ({
    orderUpdates: state.orderUpdates.filter(m => m.order_id !== order_id)
  })),
}));

const WS_URL = 'ws://localhost:8081/ws/trades'; // 按需调整
const ORDER_API = '/api/order';

const SYMBOLS = [
  { label: 'BTC/USDT', value: 'BTCUSDT' },
  { label: 'ETH/USDT', value: 'ETHUSDT' },
  { label: 'BNB/USDT', value: 'BNBUSDT' },
];

const TradePage: React.FC = () => {
  // 下单表单状态
  const [price, setPrice] = useState('');
  const [amount, setAmount] = useState('');
  const [side, setSide] = useState('buy');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [symbol, setSymbol] = useState('BTCUSDT');

  // 多币种成交缓存
  const [tradeCache, setTradeCache] = useState<{ [symbol: string]: Trade[] }>({});

  // Zustand
  const trades = tradeCache[symbol] || [];
  const addTrade = (trade: Trade) => {
    setTradeCache((cache) => {
      const list = [trade, ...(cache[trade.symbol] || [])].slice(0, 50);
      return { ...cache, [trade.symbol]: list };
    });
  };
  const setTrades = (trades: Trade[]) => {
    setTradeCache((cache) => ({ ...cache, [symbol]: trades.slice(0, 50) }));
  };

  // 下单提交
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSuccess('');
    try {
      const resp = await fetch(ORDER_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ symbol, price: Number(price), amount: Number(amount), side }),
      });
      if (!resp.ok) throw new Error('下单失败');
      setSuccess('下单成功');
      setPrice('');
      setAmount('');
    } catch (err: any) {
      setError(err.message || '下单异常');
    } finally {
      setLoading(false);
    }
  };

  // WebSocket 频道订阅
  useEffect(() => {
    wsClient.connect(WS_URL);
    // 订阅 trade 消息
    const tradeHandler = (data: any) => {
      if (data.data && data.data.pair === symbol) {
        addTrade({
          id: data.data.trade_id || data.data.id || '',
          price: Number(data.data.price),
          amount: Number(data.data.quantity || data.data.amount),
          side: data.data.side,
          ts: String(data.data.timestamp || Date.now()),
          symbol: data.data.pair || symbol,
        });
      }
    };
    wsClient.subscribe('trade', tradeHandler);

    // 订阅订单状态变更（order_update）消息
    const orderUpdateHandler = (data: any) => {
      if (data.data && data.data.order_id) {
        addOrderUpdate({
          order_id: data.data.order_id,
          status: data.data.status,
          ...data.data
        });
      }
    };
    wsClient.subscribe('order_update', orderUpdateHandler);

    return () => {
      wsClient.unsubscribe('trade', tradeHandler);
      wsClient.unsubscribe('order_update', orderUpdateHandler);
    };
    // eslint-disable-next-line
  }, [symbol]);

  const { orderUpdates, removeOrderUpdate } = useOrderUpdateStore();

  return (
    <div className="max-w-xl mx-auto p-6 space-y-8">
      {/* 订单状态变更消息条 */}
      {orderUpdates.length > 0 && (
        <div className="space-y-2 mb-4">
          {orderUpdates.map(msg => (
            <div key={msg.order_id} className="bg-blue-50 border border-blue-200 text-blue-800 px-4 py-2 rounded flex items-center justify-between shadow">
              <span>订单号 {msg.order_id} 状态：{msg.status}</span>
              <button className="ml-4 text-blue-500 hover:underline" onClick={() => removeOrderUpdate(msg.order_id)}>关闭</button>
            </div>
          ))}
        </div>
      )}
      <div>
        <h2 className="text-xl font-bold mb-4">下单</h2>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <Label htmlFor="symbol">币种</Label>
            <Select value={symbol} onValueChange={setSymbol}>
              <SelectTrigger id="symbol">
                <SelectValue placeholder="选择币种" />
              </SelectTrigger>
              <SelectContent>
                {SYMBOLS.map((s) => (
                  <SelectItem key={s.value} value={s.value}>{s.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div>
            <Label htmlFor="price">价格</Label>
            <Input id="price" type="number" value={price} onChange={e => setPrice(e.target.value)} required step="0.01" />
          </div>
          <div>
            <Label htmlFor="amount">数量</Label>
            <Input id="amount" type="number" value={amount} onChange={e => setAmount(e.target.value)} required step="0.0001" />
          </div>
          <div>
            <Label htmlFor="side">方向</Label>
            <Select value={side} onValueChange={setSide}>
              <SelectTrigger id="side">
                <SelectValue placeholder="选择方向" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="buy">买入</SelectItem>
                <SelectItem value="sell">卖出</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <Button type="submit" disabled={loading} className="w-full">提交</Button>
          {error && <div className="text-red-500 text-sm">{error}</div>}
          {success && <div className="text-green-600 text-sm">{success}</div>}
        </form>
      </div>
      <div>
        <h2 className="text-xl font-bold mb-4">实时成交</h2>
        <div className="border rounded-md overflow-auto max-h-80">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>时间</TableHead>
                <TableHead>方向</TableHead>
                <TableHead>价格</TableHead>
                <TableHead>数量</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {trades.length === 0 && (
                <TableRow><TableCell colSpan={4}>暂无成交</TableCell></TableRow>
              )}
              {trades.map(trade => (
                <TableRow key={trade.id}>
                  <TableCell>{trade.ts}</TableCell>
                  <TableCell className={trade.side === 'buy' ? 'text-green-600' : 'text-red-500'}>
                    {trade.side === 'buy' ? '买入' : '卖出'}
                  </TableCell>
                  <TableCell>{trade.price}</TableCell>
                  <TableCell>{trade.amount}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  );
};

export default TradePage;
