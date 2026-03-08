const { Client } = require('pg');

const BATCH_LIMIT = 1000;
const TABLE = 'hktvmall.hktvmallDaily';

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

  const rawItems = Array.isArray(body.items) ? body.items : Array.isArray(body) ? body : null;
  if (!rawItems || rawItems.length === 0) {
    // Auth passed and body is valid JSON, but there is nothing to insert.
    return res.status(200).json({
      success: true,
      message: 'No items to insert. 0 item(s) processed.',
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

  // Helper function to normalize object keys to lowercase for case-insensitive access
  const normalizeKeys = (obj) => {
    const normalized = {};
    for (const key in obj) {
      if (obj.hasOwnProperty(key)) {
        normalized[key.toLowerCase()] = obj[key];
      }
    }
    return normalized;
  };

  const normalized = rawItems.map((item, index) => {
    const itemLower = normalizeKeys(item);
    return {
      index,
      storeId: itemLower.storeid != null ? String(itemLower.storeid) : null,
      itemId: itemLower.itemid != null ? String(itemLower.itemid) : null,
      itemName: itemLower.itemname != null ? String(itemLower.itemname) : null,
      itemURL: itemLower.itemurl != null ? String(itemLower.itemurl) : null,
      itemPrice_Curr: itemLower.itemprice_curr != null ? String(itemLower.itemprice_curr) : null,
      itemOthers: itemLower.itemothers != null ? (typeof itemLower.itemothers === 'string' ? itemLower.itemothers : JSON.stringify(itemLower.itemothers)) : null,
      imageUrl: itemLower.imageurl != null ? String(itemLower.imageurl) : null,
      category: itemLower.category != null ? (typeof itemLower.category === 'string' ? itemLower.category : JSON.stringify(itemLower.category)) : null,
      countryOfOrigin: itemLower.countryoforigin != null ? String(itemLower.countryoforigin) : null,  // ← Changed to camelCase
      purchasable: itemLower.purchasable != null ? String(itemLower.purchasable) : null,
      salesVolume: itemLower.salesvolume != null ? String(itemLower.salesvolume) : null,            // ← Changed
      numberOfReviews: itemLower.numberofreviews != null ? String(itemLower.numberofreviews) : null,
      averageRating: itemLower.averagerating != null ? String(itemLower.averagerating) : null,
      recordDatetime: itemLower.recorddatetime ? new Date(itemLower.recorddatetime) : new Date()
    };
  });

  

  const invalid = normalized.filter(
    (r) => r.itemId === null || r.itemName === null || r.itemURL === null || r.itemPrice_Curr === null
  );
  if (invalid.length > 0) {
    return res.status(400).json({
      success: false,
      error: 'Each item must have itemId, itemName, itemURL, itemPrice_Curr.',
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
      placeholders.push(
        `($${paramIndex}, $${paramIndex + 1}, $${paramIndex + 2}, $${paramIndex + 3}, $${paramIndex + 4}, $${paramIndex + 5}, $${paramIndex + 6}, $${paramIndex + 7}, $${paramIndex + 8}, $${paramIndex + 9}, $${paramIndex + 10}, $${paramIndex + 11}, $${paramIndex + 12}, $${paramIndex + 13})`
      );
      values.push(
        row.storeId,
        row.itemId,
        row.itemName,
        row.itemURL,
        row.itemPrice_Curr,
        row.itemOthers,
        row.imageUrl,
        row.category,
        row.countryOfOrigin,
        row.purchasable,
        row.salesVolume,
        row.numberOfReviews,
        row.averageRating,
        row.recordDatetime
      );
      paramIndex += 14;
    }

    const sql = `
      INSERT INTO ${TABLE}
        (storeid, itemid, itemname, itemurl, itemprice_curr, itemothers, imageurl, category, countryoforigin, purchasable, salesvolume, numberofreviews, averagerating, recorddatetime)
      VALUES ${placeholders.join(', ')}
    `;
    await client.query(sql, values);
    await client.end();

    return res.status(200).json({
      success: true,
      message: `Inserted ${normalized.length} item(s).`,
      insertedCount: normalized.length,
      failedCount: 0,
      errors: [],
      batchLimit: BATCH_LIMIT
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}
    console.error('insertItem error:', err.message);
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
