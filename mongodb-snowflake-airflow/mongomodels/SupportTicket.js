const mongoose = require("mongoose");

const historyEntrySchema = new mongoose.Schema({
  action: String,
  timestamp: { type: Date, default: Date.now },
});

const attachmentRequestSchema = new mongoose.Schema({
  requestedBy: String,
  message: String,
  requestedAt: { type: Date, default: Date.now },
  fulfilled: { type: Boolean, default: false },
});

const supportTicketSchema = new mongoose.Schema({
  supportTicketId: {
    type: String,
    required: true,
    unique: true,
    match: /^SUPID-\d{6}-[A-Z0-9]{8}$/i, // updated pattern
  },
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "Supplier",
    required: true,
  },
  issue: String,
  channel: {
    type: String,
    enum: ["email", "call", "chat"],
    required: true,
  },
  status: {
    type: String,
    enum: ["Open", "InProgress", "Resolved"],
    default: "Open",
  },
  resolution: {
    type: String,
    default: null,
  },
  priority: {
    type: String,
    enum: ["Low", "Medium", "High", "Critical"],
    default: "Medium",
  },
  comments: [
    {
      text: String,
      timestamp: { type: Date, default: Date.now },
    },
  ],
  attachments: [
    {
      publicId: String,
      url: String,
      filename: String,
      uploadedAt: { type: Date, default: Date.now },
    },
  ],
  attachmentRequests: [attachmentRequestSchema],
  history: [historyEntrySchema],
  customerSatisfaction: { type: Number, min: 1, max: 5 },
  tags: [String],
  dueDate: { type: Date, default: null },
  escalated: { type: Boolean, default: false },
  chatSessionId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: "ChatSession",
    default: null,
  },
  createdAt: { type: Date, default: Date.now },
});

module.exports = mongoose.model("SupportTicket", supportTicketSchema);
