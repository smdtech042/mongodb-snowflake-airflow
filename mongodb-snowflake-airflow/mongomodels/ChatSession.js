const mongoose = require("mongoose");

const messageSchema = new mongoose.Schema({
  sender: { type: String, required: true },
  text: String,
  timestamp: { type: Date, default: Date.now },
});

const chatSessionSchema = new mongoose.Schema({
  ticketId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "SupportTicket",
    required: true,
  },
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  agentId: {
    type: String,
    default: null,
  },
  messages: [messageSchema],
  status: {
    type: String,
    enum: ["active", "waiting", "closed", "converted"],
    default: "waiting",
  },
  startedAt: { type: Date, default: Date.now },
  endedAt: { type: Date, default: null },
});

module.exports = mongoose.model("ChatSession", chatSessionSchema);
