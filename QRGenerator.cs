using Core;
using DCJobs;
using Nest;
using QRCoder;
using System;
using System.Collections.Generic;
using System.Drawing.Drawing2D;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static QRCoder.PayloadGenerator;
using System.Drawing.Imaging;
using Core.Models;
using Newtonsoft.Json.Linq;

namespace JobWorker
{
    public class QRGenerator
    {
        public QRGenerator() { }
        public void GenerateQR(document doc, qrcode oQRcode) {

            //JObject reqData = JObject.Parse(reqStr);

            //string docid = reqData["docid"].Value<string>();
            int qrsize = oQRcode.qrsize;
            string qrtext = oQRcode.text;
            string qr_filename = oQRcode.filename;
            //string qrurl = reqData["qrurl"].Value<string>();

            Url generator = new Url(oQRcode.url);
            
            string payload = generator.ToString();

            using (QRCodeGenerator qrGenerator = new QRCodeGenerator())
            using (QRCodeData qrCodeData = qrGenerator.CreateQrCode(payload, QRCodeGenerator.ECCLevel.Q))
            using (PngByteQRCode qrCode = new PngByteQRCode(qrCodeData))
            {
                byte[] qrCodeImageBytes = qrCode.GetGraphic(20);
                Bitmap qrCodeImage = new Bitmap(new MemoryStream(qrCodeImageBytes));
                Bitmap resized = new Bitmap(qrCodeImage, new Size(qrsize, qrsize));

                RectangleF rf = new RectangleF(0, qrsize - 10, qrsize, 10);
                StringFormat sf = new StringFormat
                {
                    Alignment = StringAlignment.Center,
                    LineAlignment = StringAlignment.Center
                };
                if (!string.IsNullOrEmpty(qrtext)){
                    Graphics g = Graphics.FromImage(resized);
                    g.SmoothingMode = SmoothingMode.AntiAlias;
                    g.InterpolationMode = InterpolationMode.HighQualityBicubic;
                    g.PixelOffsetMode = PixelOffsetMode.HighQuality;
                    g.DrawString(qrtext, new Font("Tahoma", 8), Brushes.Black, rf, sf);
                    g.Flush();
                }
                string sOutputDirectory = DocumentUtilBase.getDocumentPath(doc);
                sOutputDirectory = Path.Combine(sOutputDirectory, "MiniFlipper");

                if (!Directory.Exists(sOutputDirectory))
                    Directory.CreateDirectory(sOutputDirectory);

                string sOutputFileName = Path.Combine(sOutputDirectory, qr_filename);
                resized.Save(sOutputFileName, ImageFormat.Png);
            }
        }
    }
}
