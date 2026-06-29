const mongoose = require("mongoose");

const issueSchema = new mongoose.Schema(
  {
    supplier: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Supplier",
      required: true,
    },
    rm: { type: mongoose.Schema.Types.ObjectId, ref: "RM", required: true },
    title: { type: String, required: true },
    description: { type: String },
    status: {
      type: String,
      enum: ["open", "in_progress", "resolved", "closed"],
      default: "open",
    },
    priority: {
      type: String,
      enum: ["low", "medium", "high", "critical"],
      default: "medium",
    },
    type: {
      type: String,
      enum: ["technical", "account", "order", "other"],
      default: "other",
    },
  },
  { timestamps: true },
);

module.exports = mongoose.model("RMIssue", issueSchema);
