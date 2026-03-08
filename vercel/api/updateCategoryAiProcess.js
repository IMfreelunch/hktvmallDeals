const { Client } = require('pg');

const BATCH_LIMIT = 1000;
const TABLE = 'hktvmall.hktvmalldaily_latest_category_aiprocess';

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'POST') {
    return res.status(405).json({
      success: false,
      error: 'Method not allowed. Use POST with JSON body.',
      allowedMethod: 'POST'
    });
  }

  // Authorization: same pattern as insertItem.js
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

  const rawItems = Array.isArray(body?.items) ? body.items : Array.isArray(body) ? body : null;
  if (!rawItems || rawItems.length === 0) {
    return res.status(400).json({
      success: false,
      error: 'Body must contain "items" array (or a root array) with at least one item.',
      example: {
        items: [
          {
            categoryCode: '123',
            isCompleted: true
          }
        ]
      }
    });
  }

  if (rawItems.length > BATCH_LIMIT) {
    return res.status(400).json({
      success: false,
      error: `Maximum ${BATCH_LIMIT} items per request. Received ${rawItems.length}.`,
      batchLimit: BATCH_LIMIT
    });
  }

  const normalizeKeys = (obj) => {
    const normalized = {};
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        normalized[key.toLowerCase()] = obj[key];
      }
    }
    return normalized;
  };

  const normalized = rawItems.map((item, index) => {
    const lower = normalizeKeys(item);

    let isCompleted = null;
    if (typeof lower.iscompleted === 'boolean') {
      isCompleted = lower.iscompleted;
    } else if (typeof lower.iscompleted === 'string') {
      const val = lower.iscompleted.toLowerCase();
      if (val === 'true') isCompleted = true;
      else if (val === 'false') isCompleted = false;
    }

    return {
      index,
      categoryCode: lower.categorycode != null ? String(lower.categorycode) : null,
      isCompleted
    };
  });

  const invalid = normalized.filter(
    (r) => r.categoryCode === null || r.isCompleted === null
  );

  if (invalid.length > 0) {
    return res.status(400).json({
      success: false,
      error: 'Each item must have categoryCode and isCompleted (true/false).',
      invalidIndices: invalid.map((r) => r.index),
      batchLimit: BATCH_LIMIT
    });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    const placeholders = [];
    const values = [];
    let paramIndex = 1;

    for (const row of normalized) {
      placeholders.push(`($${paramIndex}, $${paramIndex + 1})`);
      values.push(row.categoryCode, row.isCompleted);
      paramIndex += 2;
    }

    const sql = `
      UPDATE ${TABLE} AS t
      SET iscompleted = (v.iscompleted::boolean)
      FROM (VALUES ${placeholders.join(', ')}) AS v(categorycode, iscompleted)
      WHERE t.categorycode = v.categorycode
    `;

    const result = await client.query(sql, values);
    await client.end();

    return res.status(200).json({
      success: true,
      message: `Updated ${result.rowCount} category AI process record(s).`,
      updatedCount: result.rowCount,
      failedCount: normalized.length - result.rowCount,
      batchLimit: BATCH_LIMIT
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}

    console.error('updateCategoryAiProcess error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Update failed.',
      error: err.message,
      updatedCount: 0,
      failedCount: normalized.length,
      batchLimit: BATCH_LIMIT
    });
  }
};

