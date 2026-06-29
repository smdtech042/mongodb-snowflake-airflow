const mongoose = require("mongoose");

const rfqResponseSchema = new mongoose.Schema({
  rfq: { type: mongoose.Schema.Types.ObjectId, ref: "RFQ", required: true },
  supplier: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  price: { type: Number, required: true },
  currency: { type: String, default: "INR" },
  deliveryTime: { type: String, required: true },
  additionalNotes: { type: String },
  files: [{ url: String }],
  status: {
    type: String,
    default: "pending",
    enum: ["pending", "accepted", "rejected"],
  },
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("RFQResponse", rfqResponseSchema);
