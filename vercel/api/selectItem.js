const { Client } = require('pg');

module.exports = async (req, res) => {
  console.log('🚀 API STARTED - itemName:', req.query?.itemName, 'all:', req.query?.all);
  console.log('📊 Environment check - DATABASE_URL exists:', !!process.env.DATABASE_URL);
  
  if (req.method !== 'GET') {
    console.log('🚫 Non-GET request blocked');
    return res.status(405).json({ error: 'GET only' });
  }
  
  try {
    const getAll = req.query?.all === 'true' || req.query?.all === '1';
    const itemName = req.query?.itemName;
    const storeId = req.query?.storeId;
    const recordDatetime = req.query?.recordDatetime;
    const limit = parseInt(req.query?.limit) || (getAll ? 10000 : 50);
    
    console.log('🔍 Mode:', getAll ? 'SELECT BY storeId/recordDatetime' : 'SEARCH BY NAME');
    console.log('🔍 Search term:', itemName || 'none');
    console.log('🔍 StoreId:', storeId || 'none');
    console.log('🔍 RecordDatetime:', recordDatetime || 'none');
    console.log('🔍 Limit:', limit);
    
    const client = new Client({
      connectionString: process.env.DATABASE_URL,
      ssl: { 
        rejectUnauthorized: false
      }
    });
    
    console.log('✅ Connecting to Neon...');
    await client.connect();
    console.log('✅ Neon connection successful!');
    
    let result;
    
    if (getAll) {
      // 🔍 Select records filtered by storeId and/or recordDatetime_max
      const conditions = [];
      const values = [];
      let paramIndex = 1;
      
      if (storeId) {
        conditions.push(`storeid = $${paramIndex}`);
        values.push(storeId);
        paramIndex++;
      }
      
      if (recordDatetime) {
        conditions.push(`recordDatetime_max = $${paramIndex}`);
        values.push(new Date(recordDatetime));
        paramIndex++;
      }
      
      const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      values.push(limit);
      
      console.log('✅ Querying records from hktvmall.hktvmallDaily with storeId/recordDatetime_max filter...');
      result = await client.query(`
        SELECT DISTINCT ON (itemid)
          storeid,
          itemid,
          itemname,
          itemurl,
          itemprice_curr,
          itemothers,
          imageurl        AS "imageUrl",
          category,
          countryoforigin AS "countryOfOrigin",
          purchasable,
          salesvolume     AS "salesVolume",
          numberofreviews AS "numberOfReviews",
          averagerating   AS "averageRating",
          recorddatetime,
          recordDatetime_max,
          priceChart_max,
          priceChart_min,
          discountPercent
        FROM hktvmall.V_hktvmallDaily_latest
        ${whereClause}
        ORDER BY itemid, recorddatetime DESC
        LIMIT $${paramIndex}
      `, values);
    } else {
      // 🔍 Search by itemName (existing function for searchItem.html)
      const searchTerm = itemName && itemName !== 'none' ? itemName : 'none';
      console.log('✅ Querying hktvmall.hktvmallDaily with itemName filter...');
      result = await client.query(`
        SELECT DISTINCT ON (itemid)
          storeid,
          itemid,
          itemname,
          itemurl,
          itemprice_curr,
          itemothers,
          imageurl        AS "imageUrl",
          category,
          countryoforigin AS "countryOfOrigin",
          purchasable,
          salesvolume     AS "salesVolume",
          numberofreviews AS "numberOfReviews",
          averagerating   AS "averageRating",
          recorddatetime,
          recordDatetime_max,
          priceChart_max,
          priceChart_min,
          discountPercent
        FROM hktvmall.V_hktvmallDaily_latest
        WHERE itemname ILIKE $1
        ORDER BY itemid, recorddatetime DESC
        LIMIT $2
      `, [`%${searchTerm}%`, limit]);
    }
    
    await client.end();
    console.log('✅ Database connection closed');
    
    console.log('✅ Found', result.rows.length, 'rows');
    
    const searchInfo = getAll 
      ? (storeId || recordDatetime 
          ? `storeId=${storeId || 'any'}, recordDatetime=${recordDatetime || 'any'}` 
          : 'ALL')
      : (itemName || 'none');
    
    res.json({
      success: true,
      count: result.rows.length,
      search: searchInfo,
      mode: getAll ? 'all' : 'search',
      limit: limit,
      filters: getAll ? { storeId: storeId || null, recordDatetime: recordDatetime || null } : { itemName: itemName || null },
      message: getAll 
        ? (storeId || recordDatetime 
            ? "✅ HKTVmall data loaded with filters!" 
            : "✅ All HKTVmall data loaded!")
        : "✅ HKTVmall data loaded!",
      data: result.rows
    });
    
  } catch (error) {
    console.error('💥 ERROR:', error.message);
    console.error('💥 ERROR Stack:', error.stack);
    const searchInfo = req.query?.all === 'true'
      ? (req.query?.storeId || req.query?.recordDatetime 
          ? `storeId=${req.query?.storeId || 'any'}, recordDatetime=${req.query?.recordDatetime || 'any'}` 
          : 'ALL')
      : (req.query?.itemName || 'none');
    
    res.status(500).json({
      success: false,
      count: 0,
      search: searchInfo,
      mode: req.query?.all === 'true' ? 'all' : 'search',
      filters: req.query?.all === 'true' 
        ? { storeId: req.query?.storeId || null, recordDatetime: req.query?.recordDatetime || null }
        : { itemName: req.query?.itemName || null },
      error: error.message,
      data: []
    });
  }
};
