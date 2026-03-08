const { Client } = require('pg');

module.exports = async (req, res) => {
  console.log('🚀 Category API STARTED - search:', req.query?.search, 'limit:', req.query?.limit);
  console.log('📊 Environment check - DATABASE_URL exists:', !!process.env.DATABASE_URL);

  if (req.method !== 'GET') {
    console.log('🚫 Non-GET request blocked');
    return res.status(405).json({ error: 'GET only' });
  }

  const mode = req.query?.mode;
  const customtype = req.query?.customtype;
  if (mode === 'filterCustom' && !customtype) {
    return res.status(400).json({
      success: false,
      error: 'customtype is required when mode=filterCustom',
      data: []
    });
  }

  const search = req.query?.search;
  const limit = parseInt(req.query?.limit, 10) || 1000;

  try {
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: {
        rejectUnauthorized: false
      }
    });

    console.log('✅ Connecting to Neon for categories...');
    await client.connect();
    console.log('✅ Neon connection for categories successful!');

    let result;

    if (mode === 'filterCustom' && customtype) {
      console.log('🔍 Querying categories by customtype:', customtype);
      result = await client.query(
        `
        SELECT DISTINCT c.categorycode, lc.categoryname
        FROM hktvmall.hktvmallcustomcategorytype c
        INNER JOIN hktvmall.V_hktvmallDaily_latest_category lc ON c.categorycode = lc.categorycode
        WHERE c.customtype = $1
        `,
        [customtype]
      );
    } else if (search === 'NEW') {
      console.log('🆕 Querying NEW categories (not yet AI processed)...');
      result = await client.query(
        `
        SELECT
          categorycode,
          categoryname
        FROM hktvmall.V_hktvmallDaily_latest_category
        WHERE categorycode NOT IN (
          SELECT categorycode
          FROM hktvmall.hktvmallDaily_latest_category_AIprocess
        )
        ORDER BY categorycode
      `
      );
    } else if (search && search !== 'ALL') {
      console.log('🔍 Querying categories with name filter...');
      result = await client.query(
        `
        SELECT
          categorycode,
          categoryname
        FROM hktvmall.V_hktvmallDaily_latest_category
        WHERE categoryname ILIKE $1
        ORDER BY categorycode
        LIMIT $2
      `,
        [`%${search}%`, limit]
      );
    } else {
      console.log('🔍 Querying all categories...');
      result = await client.query(
        `
        SELECT
          categorycode,
          categoryname
        FROM hktvmall.V_hktvmallDaily_latest_category
        ORDER BY categorycode
        LIMIT $1
      `,
        [limit]
      );
    }

    await client.end();
    console.log('✅ Category database connection closed');
    console.log('✅ Found', result.rows.length, 'category rows');

    res.json({
      success: true,
      count: result.rows.length,
      search: mode === 'filterCustom' ? 'filterCustom' : (search || 'ALL'),
      limit,
      ...(mode === 'filterCustom' && { customtype }),
      message: '✅ Category data loaded!',
      data: result.rows
    });
  } catch (error) {
    console.error('💥 CATEGORY API ERROR:', error.message);
    console.error('💥 CATEGORY API ERROR Stack:', error.stack);

    res.status(500).json({
      success: false,
      count: 0,
      search: search || 'ALL',
      limit,
      error: error.message,
      data: []
    });
  }
};

