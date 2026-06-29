const mongoose = require("mongoose");

const rmDocumentSchema = new mongoose.Schema(
  {
    supplier: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supplier",
      required: true,
    },
    rm: { type: mongoose.Schema.Types.ObjectId, ref: "RM", required: true },
    title: { type: String, required: true },
    type: {
      type: String,
      enum: ["contract", "report", "policy", "other"],
      default: "other",
    },
    url: { type: String, required: true },
    uploadedAt: { type: Date, default: Date.now },
  },
  { timestamps: true },
);

module.exports = mongoose.model("RMDocument", rmDocumentSchema);
