import { NostrEvent, PeerSignalMessage } from '../types/index.js';

export const OROGEN_SIGNAL_KIND = 29333; // Ephemeral Kind for Orogen WebRTC rendezvous

export interface NostrSignalCallbacks {
  onSignalReceived: (signal: PeerSignalMessage) => void;
  onRelayStatusChange?: (relay: string, connected: boolean) => void;
}

export class NostrSignalingMesh {
  private pubkey: string;
  private relays: string[];
  private sockets: Map<string, WebSocket> = new Map();
  private callbacks: NostrSignalCallbacks;

  constructor(pubkey: string, relays: string[] = ['wss://relay.damus.io', 'wss://nos.lol'], callbacks: NostrSignalCallbacks) {
    this.pubkey = pubkey;
    this.relays = relays;
    this.callbacks = callbacks;
  }

  /**
   * Connects to configured Nostr relays and subscribes to ephemeral Kind 29333 signaling events
   */
  async connect(): Promise<void> {
    for (const relayUrl of this.relays) {
      try {
        if (typeof WebSocket === 'undefined') continue;

        const ws = new WebSocket(relayUrl);

        ws.onopen = () => {
          this.callbacks.onRelayStatusChange?.(relayUrl, true);
          // Subscribe to Kind 29333 events tagged with this peer's pubkey
          const subFilter = {
            kinds: [OROGEN_SIGNAL_KIND],
            '#p': [this.pubkey],
          };
          const reqMsg = JSON.stringify(['REQ', `sub_${this.pubkey.slice(0, 8)}`, subFilter]);
          ws.send(reqMsg);
        };

        ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            if (data[0] === 'EVENT' && data[2]) {
              this.handleNostrEvent(data[2]);
            }
          } catch (e) {
            console.warn('Error parsing incoming Nostr frame:', e);
          }
        };

        ws.onerror = () => {
          this.callbacks.onRelayStatusChange?.(relayUrl, false);
        };

        ws.onclose = () => {
          this.callbacks.onRelayStatusChange?.(relayUrl, false);
          this.sockets.delete(relayUrl);
        };

        this.sockets.set(relayUrl, ws);
      } catch (err) {
        console.warn(`Failed to connect to Nostr relay ${relayUrl}:`, err);
      }
    }
  }

  /**
   * Broadcasts an SDP offer/answer or ICE candidate to a target peer via Kind 29333 ephemeral event
   */
  async sendSignal(toPubkey: string, datasetId: string, signal: { type: 'offer' | 'answer' | 'candidate'; payload: any }): Promise<void> {
    const message: PeerSignalMessage = {
      type: signal.type,
      fromPubkey: this.pubkey,
      toPubkey,
      datasetId,
      payload: signal.payload,
      timestamp: Date.now(),
    };

    const nostrEvent: NostrEvent = {
      pubkey: this.pubkey,
      created_at: Math.floor(Date.now() / 1000),
      kind: OROGEN_SIGNAL_KIND,
      tags: [
        ['p', toPubkey],
        ['d', datasetId],
        ['t', 'orogen-signal'],
      ],
      content: JSON.stringify(message),
    };

    const payload = JSON.stringify(['EVENT', nostrEvent]);

    let sent = false;
    for (const [_, ws] of this.sockets) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(payload);
        sent = true;
      }
    }

    if (!sent) {
      // If offline or in mock test environment, loop back to callback for local peer testing
      if (toPubkey === this.pubkey || toPubkey === 'local_mock_peer') {
        setTimeout(() => this.callbacks.onSignalReceived(message), 10);
      }
    }
  }

  private handleNostrEvent(event: NostrEvent): void {
    if (event.kind !== OROGEN_SIGNAL_KIND) return;

    try {
      const signal: PeerSignalMessage = JSON.parse(event.content);
      if (signal.toPubkey === this.pubkey) {
        this.callbacks.onSignalReceived(signal);
      }
    } catch (e) {
      console.warn('Invalid Nostr event signal content:', e);
    }
  }

  disconnect(): void {
    for (const [_, ws] of this.sockets) {
      try {
        ws.close();
      } catch {}
    }
    this.sockets.clear();
  }
}
