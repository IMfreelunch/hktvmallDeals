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
    // Auth passed and body is valid JSON, but there is nothing to insert.
    return res.status(200).json({
      success: true,
      message: 'No category AI process records to insert. 0 record(s) processed.',
      insertedCount: 0,
      failedCount: 0,
      errors: [],
      batchLimit: BATCH_LIMIT
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

    if (isCompleted === null) {
      // default to false if not provided/parsable
      isCompleted = false;
    }

    return {
      index,
      categorycode: lower.categorycode != null ? String(lower.categorycode) : null,
      categoryname: lower.categoryname != null ? String(lower.categoryname) : null,
      isCompleted
    };
  });

  const invalid = normalized.filter(
    (r) => r.categorycode === null || r.categoryname === null
  );

  if (invalid.length > 0) {
    return res.status(400).json({
      success: false,
      error: 'Each item must have categorycode and categoryname.',
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
      placeholders.push(`($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2})`);
      values.push(row.categorycode, row.categoryname, row.isCompleted);
      paramIndex += 3;
    }

    const sql = `
      INSERT INTO ${TABLE} (categorycode, categoryname, iscompleted)
      VALUES ${placeholders.join(', ')}
    `;

    await client.query(sql, values);
    await client.end();

    return res.status(200).json({
      success: true,
      message: `Inserted ${normalized.length} category AI process record(s).`,
      insertedCount: normalized.length,
      failedCount: 0,
      errors: [],
      batchLimit: BATCH_LIMIT
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}

    console.error('insertCategoryAiProcess error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Insert failed.',
      error: err.message,
      insertedCount: 0,
      failedCount: normalized.length,
      errors: [{ index: -1, error: err.message }],
      batchLimit: BATCH_LIMIT
    });
  }
};

