const mongoose = require("mongoose");

const categorySchema = new mongoose.Schema({
  name: { type: String, required: true },
  customFields: [
    {
      fieldName: String,
      fieldType: { type: String, enum: ["String", "Number", "Boolean"] },
      required: { type: Boolean, default: false },
    },
  ],
});

module.exports = mongoose.model("Category", categorySchema);
