/**
 * Orogen Virtual Apache Iceberg Catalog Service Worker Interceptor
 * Intercepts HTTP requests directed to https://orogen.local/api/v1/...
 * Serves Iceberg metadata JSON and streams Parquet chunk files directly from OPFS
 * using asynchronous read-only getFile() streams (Directive 12: No File Locking).
 */

const OROGEN_API_ORIGIN = 'https://orogen.local';

self.addEventListener('install', (event: any) => {
  event.waitUntil((self as any).skipWaiting());
});

self.addEventListener('activate', (event: any) => {
  event.waitUntil((self as any).clients.claim());
});

self.addEventListener('fetch', (event: any) => {
  const url = new URL(event.request.url);

  if (url.origin === OROGEN_API_ORIGIN && url.pathname.startsWith('/api/v1/')) {
    event.respondWith(handleOrogenCatalogRequest(url, event.request));
  }
});

async function handleOrogenCatalogRequest(url: URL, _request: Request): Promise<Response> {
  const path = url.pathname.replace('/api/v1/', '');

  try {
    // 1. Iceberg Catalog Configuration endpoint
    if (path === 'config') {
      return new Response(
        JSON.stringify({
          'overrides': {
            'warehouse': 'https://orogen.local/api/v1/warehouse',
          },
          'defaults': {
            'clients': 'duckdb-wasm',
          },
        }),
        {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cross-Origin-Resource-Policy': 'cross-origin',
          },
        }
      );
    }

    // 2. Iceberg Namespaces
    if (path === 'namespaces') {
      return new Response(
        JSON.stringify({
          namespaces: [['default'], ['quant'], ['macro']],
        }),
        {
          status: 200,
          headers: {
            'Content-Type': 'application/json',
            'Cross-Origin-Resource-Policy': 'cross-origin',
          },
        }
      );
    }

    // 3. Iceberg Table Metadata endpoint: /namespaces/{ns}/tables/{table}
    const tableMatch = path.match(/^namespaces\/([^/]+)\/tables\/([^/]+)$/);
    if (tableMatch) {
      const [, namespace, table] = tableMatch;
      const metadata = generateIcebergTableMetadata(namespace!, table!);
      return new Response(JSON.stringify(metadata), {
        status: 200,
        headers: {
          'Content-Type': 'application/json',
          'Cross-Origin-Resource-Policy': 'cross-origin',
        },
      });
    }

    // 4. Warehouse Parquet Data Stream: /warehouse/{datasetId}/{chunkName}
    const warehouseMatch = path.match(/^warehouse\/([^/]+)\/([^/]+)$/);
    if (warehouseMatch) {
      const [, datasetId, fileName] = warehouseMatch;

      // Access OPFS using non-locking read-only getFile() stream
      const rootDir = await navigator.storage.getDirectory();
      const datasetDir = await rootDir.getDirectoryHandle(datasetId!, { create: false });
      const fileHandle = await datasetDir.getFileHandle(fileName!, { create: false });
      const file = await fileHandle.getFile();

      return new Response(file.stream(), {
        status: 200,
        headers: {
          'Content-Type': 'application/vnd.apache.parquet',
          'Content-Length': file.size.toString(),
          'Cross-Origin-Resource-Policy': 'cross-origin',
        },
      });
    }

    return new Response(JSON.stringify({ error: 'Endpoint not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (err: any) {
    return new Response(JSON.stringify({ error: err.message || 'Internal catalog interceptor error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

function generateIcebergTableMetadata(namespace: string, table: string) {
  return {
    'format-version': 2,
    'table-uuid': `${namespace}-${table}-uuid`,
    'location': `https://orogen.local/api/v1/warehouse/${table}`,
    'last-sequence-number': 1,
    'last-updated-ms': Date.now(),
    'last-column-id': 3,
    'current-schema-id': 0,
    'schemas': [
      {
        'type': 'struct',
        'schema-id': 0,
        'fields': [
          { 'id': 1, 'name': 'timestamp', 'required': true, 'type': 'timestamptz' },
          { 'id': 2, 'name': 'value', 'required': true, 'type': 'double' },
        ],
      },
    ],
    'current-snapshot-id': 1,
    'snapshots': [
      {
        'snapshot-id': 1,
        'timestamp-ms': Date.now(),
        'summary': { 'operation': 'append' },
        'manifest-list': `https://orogen.local/api/v1/warehouse/${table}/manifest-list.avro`,
      },
    ],
  };
}
