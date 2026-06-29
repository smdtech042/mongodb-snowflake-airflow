const mongoose = require("mongoose");
const Counter = require("./Counter");

const rfqSchema = new mongoose.Schema({
  rfqId: { type: String, unique: true },
  user: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "ClientUser",
    required: true,
  },
  productName: { type: String, required: true },
  productCategory: { type: String, required: true },
  productDetails: { type: String, required: true },
  files: [{ url: String }],
  orderQuantity: { type: Number, required: true },
  unit: { type: String, required: true },
  email: { type: String, required: true },
  autoSendAfter48h: { type: Boolean, default: false },
  suppliersNotified: [
    { type: mongoose.Schema.Types.ObjectId, ref: "Supplier" },
  ],
  status: { type: String, default: "pending" },
  createdAt: { type: Date, default: Date.now },
});

rfqSchema.pre("save", async function (next) {
  if (!this.rfqId) {
    try {
      const counter = await Counter.findOneAndUpdate(
        { name: "rfqId" },
        { $inc: { count: 1 } },
        { new: true, upsert: true }
      );
      this.rfqId = `RFQ${String(counter.count).padStart(6, "0")}`;
      next();
    } catch (error) {
      next(error);
    }
  } else {
    next();
  }
});

// rfqSchema.index({ rfqId: 1 }, { unique: true });

module.exports = mongoose.model("RFQ", rfqSchema);
