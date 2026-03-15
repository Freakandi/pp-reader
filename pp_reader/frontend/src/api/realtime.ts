/**
 * Transport-agnostic real-time client interface + SSE implementation.
 * Decision 5: WebSocket-ready design — components depend on RealtimeClient,
 * not on EventSource or WebSocket directly.
 */

export type EventHandler = (data: unknown) => void;

/**
 * Transport-agnostic interface for real-time event delivery.
 * Initial implementation: SSEClient.
 * Future: WebSocketClient (adds bidirectional send()).
 */
export interface RealtimeClient {
  connect(): void;
  disconnect(): void;
  onEvent(type: string, handler: EventHandler): void;
  /** Optional — present on WebSocket implementation only. */
  send?(command: string, payload: unknown): void;
}

/**
 * SSE-based implementation of RealtimeClient.
 * EventSource auto-reconnects on network failure.
 */
export class SSEClient implements RealtimeClient {
  private readonly url: string;
  private source: EventSource | null = null;
  /** Map of event type → list of raw MessageEvent listeners attached to source. */
  private readonly attached = new Map<string, EventListener>();
  /** Map of event type → registered app-level handlers. */
  private readonly handlers = new Map<string, EventHandler[]>();

  constructor(url = '/api/events') {
    this.url = url;
  }

  connect(): void {
    if (this.source) return;
    this.source = new EventSource(this.url);
    this.source.onerror = () => {
      console.warn('SSEClient: connection error — EventSource will auto-reconnect');
    };
    // Attach all pre-registered event types
    for (const type of this.handlers.keys()) {
      this._attach(type);
    }
  }

  disconnect(): void {
    this.source?.close();
    this.source = null;
    this.attached.clear();
  }

  onEvent(type: string, handler: EventHandler): void {
    const existing = this.handlers.get(type);
    if (existing) {
      existing.push(handler);
    } else {
      this.handlers.set(type, [handler]);
      if (this.source) {
        this._attach(type);
      }
    }
  }

  private _attach(type: string): void {
    if (!this.source || this.attached.has(type)) return;
    const listener: EventListener = (evt: Event) => {
      const data = (evt as MessageEvent<string>).data;
      const handlers = this.handlers.get(type) ?? [];
      let parsed: unknown;
      try {
        parsed = JSON.parse(data) as unknown;
      } catch {
        parsed = data;
      }
      for (const h of handlers) {
        h(parsed);
      }
    };
    this.source.addEventListener(type, listener);
    this.attached.set(type, listener);
  }
}
