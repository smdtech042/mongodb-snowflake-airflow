const mongoose = require("mongoose");

const RMSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    email: { type: String, required: true, unique: true },
    phone: { type: String },
    assigned_suppliers: [
      { type: mongoose.Schema.Types.ObjectId, ref: "Supplier" },
    ],
    status: { type: String, default: "active" },
  },
  { timestamps: true },
);

module.exports = mongoose.model("RM", RMSchema);
