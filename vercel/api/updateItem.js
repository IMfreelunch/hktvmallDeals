const { Client } = require('pg');

const BATCH_LIMIT = 1000;
const TABLE = 'hktvmall.hktvmallDaily';
const KEY_COLUMNS = ['itemId', 'recordDatetime'];
const UPDATABLE_COLUMNS = ['storeId', 'itemName', 'itemURL', 'itemPrice_Curr', 'itemOthers'];

// Try camelCase first, then lowercase (e.g. for n8n / DB-style keys)
function getProp(obj, ...keys) {
  if (obj == null) return undefined;
  for (const k of keys) {
    if (obj[k] !== undefined && obj[k] !== null) return obj[k];
  }
  return undefined;
}

const COL_ALIASES = {
  storeId: ['storeId', 'storeid'],
  itemName: ['itemName', 'itemname'],
  itemURL: ['itemURL', 'itemurl'],
  itemPrice_Curr: ['itemPrice_Curr', 'itemprice_curr'],
  itemOthers: ['itemOthers', 'itemothers']
};

// DB column names (table uses lowercase)
const DB_COLUMNS = {
  storeId: 'storeid',
  itemName: 'itemname',
  itemURL: 'itemurl',
  itemPrice_Curr: 'itemprice_curr',
  itemOthers: 'itemothers'
};

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'POST') {
    return res.status(405).json({
      success: false,
      error: 'Method not allowed. Use POST with JSON body.',
      allowedMethod: 'POST'
    });
  }

  // Authorization: require neonDirectApiToken (header: Authorization Bearer <token> or x-api-key)
  const expectedToken = process.env.neonDirectApiToken;
  if (!expectedToken) {
    return res.status(503).json({
      success: false,
      error: 'Authorization not configured (neonDirectApiToken missing).'
    });
  }
  const authHeader = (req.headers.authorization || '').trim();
  const bearerToken = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : null;
  const apiKey = (req.headers['x-api-key'] || '').trim();
  const providedToken = bearerToken || apiKey || null;
  if (!providedToken || providedToken !== expectedToken) {
    return res.status(401).json({
      success: false,
      error: 'Unauthorized. Provide a valid token in Authorization: Bearer <token> or x-api-key header.'
    });
  }

  let body;
  try {
    body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
  } catch (e) {
    return res.status(400).json({
      success: false,
      error: 'Invalid JSON body.',
      detail: e.message
    });
  }

  const hasKeys = (o) => getProp(o, 'itemId', 'itemid') != null && getProp(o, 'recordDatetime', 'recorddatetime') != null;
  const rawUpdates = Array.isArray(body.updates) ? body.updates : Array.isArray(body) ? body : (body && hasKeys(body) ? [body] : null);
  if (!rawUpdates || rawUpdates.length === 0) {
    return res.status(400).json({
      success: false,
      error: 'Body must contain "updates" array, a root array, or a single object with itemId/itemid and recordDatetime/recorddatetime.',
      example: { updates: [{ itemId: '...', recordDatetime: '...', itemName: 'New Name', itemPrice_Curr: '99.00' }] }
    });
  }

  if (rawUpdates.length > BATCH_LIMIT) {
    return res.status(400).json({
      success: false,
      error: `Maximum ${BATCH_LIMIT} update(s) per request. Received ${rawUpdates.length}.`,
      batchLimit: BATCH_LIMIT
    });
  }

  const normalized = rawUpdates.map((row, index) => {
    const rawItemId = getProp(row, 'itemId', 'itemid');
    const rawRecordDatetime = getProp(row, 'recordDatetime', 'recorddatetime');
    const itemId = rawItemId != null ? String(rawItemId) : null;
    const recordDatetime = rawRecordDatetime != null ? new Date(rawRecordDatetime) : null;
    const setFields = {};
    for (const col of UPDATABLE_COLUMNS) {
      const val = getProp(row, ...COL_ALIASES[col]);
      if (val !== undefined && val !== null) {
        setFields[col] = typeof val === 'object' && col !== 'itemOthers' ? String(val) : (col === 'itemOthers' && typeof val !== 'string' ? JSON.stringify(val) : val);
      }
    }
    return { index, itemId, recordDatetime, setFields, setKeys: Object.keys(setFields) };
  });

  const missingKey = normalized.find((r) => r.itemId === null || r.recordDatetime === null);
  if (missingKey) {
    return res.status(400).json({
      success: false,
      error: 'Each update must have itemId and recordDatetime (key columns).',
      invalidIndices: normalized.filter((r) => r.itemId === null || r.recordDatetime === null).map((r) => r.index),
      batchLimit: BATCH_LIMIT
    });
  }

  const noSetColumns = normalized.filter((r) => r.setKeys.length === 0);
  if (noSetColumns.length > 0) {
    return res.status(400).json({
      success: false,
      error: 'Each update must specify at least one column to update: itemName, itemURL, itemPrice_Curr, or itemOthers.',
      invalidIndices: noSetColumns.map((r) => r.index),
      allowedColumns: UPDATABLE_COLUMNS,
      batchLimit: BATCH_LIMIT
    });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    let updatedCount = 0;
    const errors = [];

    for (const row of normalized) {
      try {
        const setClauses = [];
        const values = [];
        let paramIndex = 1;
        const colsToSet = UPDATABLE_COLUMNS.filter((c) => row.setFields[c] !== undefined);
        for (const col of colsToSet) {
          setClauses.push(`"${DB_COLUMNS[col]}" = $${paramIndex}`);
          values.push(row.setFields[col]);
          paramIndex += 1;
        }
        values.push(row.itemId, row.recordDatetime);
        const sql = `
          UPDATE ${TABLE}
          SET ${setClauses.join(', ')}
          WHERE "itemid" = $${paramIndex} AND "recorddatetime" = $${paramIndex + 1}
        `;
        const result = await client.query(sql, values);
        if (result.rowCount > 0) {
          updatedCount += result.rowCount;
        } else {
          errors.push({ index: row.index, error: 'No row matched itemId and recordDatetime.' });
        }
      } catch (rowErr) {
        errors.push({ index: row.index, error: rowErr.message });
      }
    }

    await client.end();

    const failedCount = normalized.length - updatedCount;
    return res.status(200).json({
      success: errors.length === 0,
      message: errors.length === 0
        ? `Updated ${updatedCount} record(s).`
        : `Updated ${updatedCount} record(s); ${errors.length} failed.`,
      updatedCount,
      failedCount,
      errors: errors.length ? errors : [],
      batchLimit: BATCH_LIMIT
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}
    console.error('updateItem error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Update failed.',
      error: err.message,
      updatedCount: 0,
      failedCount: normalized.length,
      errors: [{ index: -1, error: err.message }],
      batchLimit: BATCH_LIMIT
    });
  }
};
