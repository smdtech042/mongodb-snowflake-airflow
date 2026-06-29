const mongoose = require("mongoose");

const categorySchema = new mongoose.Schema({
  name: { type: String, required: true },
  fields: [
    {
      fieldName: { type: String, required: true },
      fieldType: {
        type: String,
        required: true,
        enum: ["text", "number", "date", "select"],
      },
      required: { type: Boolean, default: false },
      options: [String],
    },
  ],
});

module.exports = mongoose.model("Category", categorySchema);
