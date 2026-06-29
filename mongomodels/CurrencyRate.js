const mongoose = require("mongoose");

const CurrencyRateSchema = new mongoose.Schema({
  rates: { type: Object, required: true },
  updatedAt: { type: Date, required: true },
});

module.exports = mongoose.model("CurrencyRate", CurrencyRateSchema);
