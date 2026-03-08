module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.status(200).json({ 
    message: "API working! 🎉",
    database: process.env.DATABASE_URL ? "✅ Connected" : "❌ Missing",
    timestamp: new Date().toISOString()
  });
};
