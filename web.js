
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = 8080;
const publicFolder = path.join(__dirname, "public");

const server = http.createServer((req, res) => {
    const requestedPath = req.url === "/" ? "/index.html" : req.url;
    const filePath = path.join(publicFolder, requestedPath);

    if (!filePath.startsWith(publicFolder)) {
        res.writeHead(403);
        res.end("Forbidden");
        return;
    }

    fs.readFile(filePath, (error, content) => {
        if (error) {
            res.writeHead(404, { "Content-Type": "text/plain" });
            res.end("File not found");
            return;
        }

        const extension = path.extname(filePath);
        const contentTypes = {
            ".html": "text/html; charset=utf-8",
            ".css": "text/css; charset=utf-8",
            ".js": "text/javascript; charset=utf-8"
        };

        res.writeHead(200, {
            "Content-Type": contentTypes[extension] || "application/octet-stream"
        });

        res.end(content);
    });
});

server.listen(PORT, () => {
    console.log(`EduPulse web server running at http://localhost:${PORT}`);
});
