const { Client } = require('pg');

const TABLE = 'hktvmall.hktvmalldaily_latest_category_aiprocess';

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json');

  if (req.method !== 'GET') {
    return res.status(405).json({
      success: false,
      error: 'Method not allowed. Use GET with optional query parameter iscompleted=true|false.',
      allowedMethod: 'GET'
    });
  }

  // Read iscompleted filter from query string: ?iscompleted=true|false
  let isCompletedFilter = null;
  try {
    const url = new URL(req.url, 'http://localhost');
    const param = url.searchParams.get('iscompleted');
    if (param !== null) {
      const val = param.toLowerCase();
      if (val === 'true') {
        isCompletedFilter = true;
      } else if (val === 'false') {
        isCompletedFilter = false;
      } else {
        return res.status(400).json({
          success: false,
          error: 'Invalid iscompleted value. Use true or false.'
        });
      }
    }
  } catch (e) {
    return res.status(400).json({
      success: false,
      error: 'Unable to parse request URL.',
      detail: e.message
    });
  }

  const client = new Client({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  });

  try {
    await client.connect();

    let sql = `
      SELECT categorycode, categoryname, iscompleted
      FROM ${TABLE}
    `;
    const values = [];

    if (isCompletedFilter !== null) {
      sql += ' WHERE iscompleted = $1';
      values.push(isCompletedFilter);
    }

    const result = await client.query(sql, values);
    await client.end();

    return res.status(200).json({
      success: true,
      count: result.rows.length,
      records: result.rows
    });
  } catch (err) {
    try {
      await client.end();
    } catch (_) {}

    console.error('selectCategoryAiProcess error:', err.message);
    return res.status(500).json({
      success: false,
      message: 'Select failed.',
      error: err.message
    });
  }
};

