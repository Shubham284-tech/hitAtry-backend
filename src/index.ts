import express from "express";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import cors from "cors";
import helmet from "helmet";
import morgan from "morgan";
import http from "http";
import { Server } from "socket.io";
import seed from "./seed/seedDynamodb";
import * as dynamoose from "dynamoose";
import { OpenAI } from "openai";
import {
  TranscribeStreamingClient,
  StartStreamTranscriptionCommand,
} from "@aws-sdk/client-transcribe-streaming";
import { PollyClient, SynthesizeSpeechCommand } from "@aws-sdk/client-polly";

import { fromEnv } from "@aws-sdk/credential-provider-env";
import { PassThrough } from "stream";

/* ROUTE IMPORTS */
import courseRoutes from "./routes/courseRoutes";

/* CONFIGURATIONS */
dotenv.config();
const isProduction = process.env.NODE_ENV === "production";
if (!isProduction) {
  dynamoose.aws.ddb.local();
}

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const transcribeClient = new TranscribeStreamingClient({
  region: process.env.AWS_REGION,
  credentials: fromEnv(),
});

const pollyClient = new PollyClient({
  region: process.env.AWS_REGION,
  credentials: fromEnv(),
});

const app = express();
app.use(express.json());
app.use(helmet());
app.use(helmet.crossOriginResourcePolicy({ policy: "cross-origin" }));
app.use(morgan("common"));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // Use specific origin in production
    methods: ["GET", "POST"],
  },
});

function estimateAudioDurationMs(text: string) {
  const words = text.split(/\s+/).length;
  const wordsPerSecond = 2.5; // Rough estimate: 150 wpm
  return Math.ceil((words / wordsPerSecond) * 1000);
}
function generateSilenceChunk(durationMs = 500, sampleRate = 16000) {
  console.log("🤫 Injected silence to keep stream alive.");
  const silenceLength = sampleRate * (durationMs / 1000) * 2;
  return Buffer.alloc(silenceLength);
};
const userSessions = new Map();

io.on("connection", (socket) => {
  console.log("🟢 Client connected:", socket.id);

  socket.on("start_session", (data) => {
    const { industry, product, b2c, b2b, targetBuyer } = data;
    const isB2B = targetBuyer === "b2b";

    const systemPrompt = `You are participating in a virtual ${
      isB2B ? "B2B" : "B2C"
    } sales roleplay.
    This is a ${"first time"} ${"offline"} meeting.
    ${
      isB2B
        ? `You are playing the role of the buyer, who is/are ${
            b2b.persona
          } at a/an ${b2b.industry} company.
     I am a salesperson at a ${b2b.industry} company selling ${product}
     ${
       b2b.difficulty === "easy"
         ? `As the prospect you are receptive and cooperative.
          You reveal your pain points right away, making it easy to address your needs.
          Your objections are minimal and simple, providing a low-stress role-play experience.`
         : b2b.difficulty === "medium"
         ? `As the prospect you are neutral and cautious.
            You do not reveal your pain points upfront, instead framing the conversation as “shopping around” or “just ensuring you have the best options.”
            You provide decent pushback in the form of a couple of common objections, requiring the seller to navigate the conversation skillfully.`
         : `As the prospect you are challenging and resistant think of someone extremely skeptical (but still professional).
            You safeguard your pain points, requiring the seller to actively drag it out of you through probing questions and rapport-building.
            You provide multiple objections and may include a bit of snark or attitude while maintaining professionalism, testing the seller’s ability to remain composed and persuasive.`
     }`
        : ` You are playing the role of the ${b2c.customer} buyer aged ${
            b2c.age
          } with ${b2c.income} income group with focus on ${b2c.motivation}, 
    I am a salesperson at a ${industry} company selling ${product}
    
     ${
       b2c.difficulty === "easy"
         ? `As the prospect you are receptive and cooperative.
          You reveal your pain points right away, making it easy to address your needs.
          Your objections are minimal and simple, providing a low-stress role-play experience.`
         : b2c.difficulty === "medium"
         ? `As the prospect you are neutral and cautious.
            You do not reveal your pain points upfront, instead framing the conversation as “shopping around” or “just ensuring you have the best options.”
            You provide decent pushback in the form of a couple of common objections, requiring the seller to navigate the conversation skillfully.`
         : `As the prospect you are challenging and resistant think of someone extremely skeptical (but still professional).
            You safeguard your pain points, requiring the seller to actively drag it out of you through probing questions and rapport-building.
            You provide multiple objections and may include a bit of snark or attitude while maintaining professionalism, testing the seller’s ability to remain composed and persuasive.`
     }
    `
    }

    YOUR BEHAVIOR:
    - Engage with me in a realistic way.
    -Respond as a buyer till user asks for feedback. Once he asks for feedback then When that happens, analyze the entire conversation and provide feedback in the following format 
    - switch to a professional ${isB2B ? "B2B" : "B2C"} sales coach.
    -As a coach, analyze the entire conversation and provide feedback:
    -5 things done well (with keywords + explanation)
    -5 areas to improve (with keywords + explanation)
    -Final score out of 10 with justification
    -Tangible tips to improve future performance
    `;

    const userPrompt = `Hi, thanks for taking the time to meet today. I’d love to learn more about your needs and see if our ${product} might be a good fit.`;

    userSessions.set(socket.id, {
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      feedbackGiven: false,
      isProcessing: false,
    });
  });
  socket.on("user_message", async (salespersonMessage) => {
    const session = userSessions.get(socket.id);
    if (!session || session.feedbackGiven) {
      console.log("✅ Feedback already given. Ignoring input.");
      return;
    }

    if (session.isProcessing) {
      console.log("⏳ Still processing previous response.");
      return;
    }
    console.log("💬 User message:", salespersonMessage);

    session.isProcessing = true;
    session.messages.push({ role: "user", content: salespersonMessage });

    try {
      const completion = await openai.chat.completions.create({
        model: "gpt-4",
        messages: session.messages,
        stream: true,
        temperature: 0.7,
      });

      let fullReply = "";
      let bufferText = "";

      socket.emit("pause_transcription");
      for await (const chunk of completion) {
        const delta = chunk.choices[0].delta?.content;
        if (delta) {
          console.log({ delta }, "chatgpt reply");
          fullReply += delta;
          bufferText += delta;

          // Send audio for every ~20 words or end of sentence
          if (
            bufferText.split(" ").length >= 20 ||
            /[.?!]\s*$/.test(bufferText)
          ) {
            const speechResponse = await openai.audio.speech.create({
              model: "tts-1",
              voice: "coral",
              input: bufferText,
              instructions:
                "Speak in a skeptical, questioning tone, like a buyer who is unsure and probing for more information before making a decision.",
            });

            const speechBuffer = Buffer.from(
              await speechResponse.arrayBuffer()
            );
            const base64Audio = speechBuffer.toString("base64");
            console.log("here i am divided in parts find out more");
            socket.emit("gpt_audio", base64Audio);
            bufferText = ""; // reset buffer
          }

          // You can optionally send text back progressively too
          socket.emit("gpt_partial_text", fullReply);
        }
      }

      if (bufferText.trim()) {
        // Final leftover chunk
        const speechResponse = await openai.audio.speech.create({
          model: "tts-1",
          voice: "coral",
          input: bufferText,
          instructions:
            "Speak in a skeptical, questioning tone, like a buyer who is unsure and probing for more information before making a decision.",
        });

        const speechBuffer = Buffer.from(await speechResponse.arrayBuffer());
        const base64Audio = speechBuffer.toString("base64");
        socket.emit("gpt_audio", base64Audio);
      }

      session.messages.push({ role: "assistant", content: fullReply });

      setTimeout(() => {
        socket.emit("resume_transcription");
        session.isProcessing = false;
      }, estimateAudioDurationMs(fullReply));
    } catch (err) {
      console.error("❌ OpenAI error:", err);
      socket.emit(
        "gpt_reply",
        "⚠️ Sorry, there was an issue generating a response."
      );
      session.isProcessing = false;
    }
  });

  let audioStream: PassThrough;
  let lastAudioTimestamp = Date.now(); // 🆕 Track last audio chunk time
  let silenceInterval: NodeJS.Timeout;

  socket.on("start_transcription", async () => {
    console.log("🎙️ Transcription started");
    audioStream = new PassThrough();
    lastAudioTimestamp = Date.now();
    silenceInterval = setInterval(() => {
      const now = Date.now();
      if (now - lastAudioTimestamp > 10000 && audioStream) {
        const silenceChunk = generateSilenceChunk(); // inject 500ms silence
        audioStream.write(silenceChunk);
      }
    }, 5000); // check every 5 seconds
    const audioIterable = (async function* () {
      for await (const chunk of audioStream) {
        yield { AudioEvent: { AudioChunk: chunk } };
      }
    })();

    const command = new StartStreamTranscriptionCommand({
      LanguageCode: "en-US",
      MediaEncoding: "pcm",
      MediaSampleRateHertz: 16000,
      AudioStream: audioIterable,
    });

    try {
      const response = await transcribeClient.send(command);

      if (!response.TranscriptResultStream) {
        socket.emit("transcription", "⚠️ No transcript stream received.");
        return;
      }

      for await (const event of response.TranscriptResultStream) {
        const results = event.TranscriptEvent?.Transcript?.Results;
        if (results?.length && results[0].Alternatives?.length) {
          const transcript = results[0].Alternatives[0].Transcript;
          if (!results[0].IsPartial) {
            socket.emit("transcription", transcript);
          }
        }
      }
    } catch (err) {
      console.error("❌ Transcription error:", err);
      socket.emit("transcription", "⚠️ Transcription failed.");
    }
  });

  socket.on("audio_chunk", (chunk) => {
    if (audioStream && chunk) {
      audioStream.write(Buffer.from(chunk));
    }
  });

  socket.on("stop_transcription", async () => {
    console.log("🛑 Transcription stopped");
    socket.emit("pause_transcription");
    if (audioStream) {
      audioStream.end();
    }
    clearInterval(silenceInterval);
    const session = userSessions.get(socket.id);
    if (!session || session.feedbackGiven) {
      console.log("wapsiii");
      return;
    }

    try {
      session.messages.push({
        role: "user",
        content:
          "Please now switch out of character and provide your detailed feedback as per the system prompt.",
      });
      const feedbackCompletion = await openai.chat.completions.create({
        model: "gpt-4",
        messages: session.messages,
        temperature: 0.7,
      });

      const feedback = feedbackCompletion.choices[0].message.content!;
      session.messages.push({ role: "assistant", content: feedback });
      socket.emit("gpt_reply", feedback);
      console.log(feedback, "feedbackfeedback");
      // const synthCommand = new SynthesizeSpeechCommand({
      //   Text: feedback,
      //   OutputFormat: "mp3",
      //   VoiceId: "Ruth",
      //   Engine: "generative",
      // });

      // const synthResponse = await pollyClient.send(synthCommand);
      // const audioChunks: Buffer[] = [];

      // for await (const chunk of synthResponse.AudioStream as any) {
      //   audioChunks.push(chunk);
      // }

      // const audioBuffer = Buffer.concat(audioChunks);
      // const base64Audio = audioBuffer.toString("base64");
      // console.log("sun paara hoon main");
      // socket.emit("gpt_audio", base64Audio);
      setTimeout(() => {
        socket.disconnect(true);
      }, 2000);
      session.feedbackGiven = true;
      session.isProcessing = false;
    } catch (err) {
      console.error("❌ Feedback generation error:", err);
      socket.emit("gpt_reply", "⚠️ Failed to generate feedback.");
      session.isProcessing = false;
    }
  });

  socket.on("disconnect", () => {
    console.log("🔴 Client disconnected:", socket.id);
    userSessions.delete(socket.id);
  });
});

app.get("/", (req, res) => {
  res.send("Hello World from shubham");
});

app.use("/courses", courseRoutes);

/* SERVER */
const port = process.env.PORT || 3000;
if (!isProduction) {
  server.listen(port, () => {
    console.log(`🚀 Server with Socket.IO running on http://localhost:${port}`);
  });
}
