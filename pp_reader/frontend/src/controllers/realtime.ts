/**
 * RealtimeController — Lit ReactiveController that manages SSE connection
 * lifecycle tied to the host component.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import { SSEClient } from '../api/realtime.js';
import type { RealtimeClient, EventHandler } from '../api/realtime.js';

export class RealtimeController implements ReactiveController {
  private readonly host: ReactiveControllerHost;
  readonly client: RealtimeClient;

  constructor(host: ReactiveControllerHost, client?: RealtimeClient) {
    this.host = host;
    this.client = client ?? new SSEClient();
    host.addController(this);
  }

  hostConnected(): void {
    this.client.connect();
  }

  hostDisconnected(): void {
    this.client.disconnect();
  }

  /**
   * Register a handler for a named SSE event type.
   * The host will request a re-render after each event.
   */
  onEvent(type: string, handler: EventHandler): void {
    this.client.onEvent(type, (data) => {
      handler(data);
      this.host.requestUpdate();
    });
  }
}
