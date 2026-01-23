using System;
using System.Collections.Generic;
using iText.Kernel.Geom;
using iText.Kernel.Pdf;
using PdfDocument = iText.Kernel.Pdf.PdfDocument;

public class PDFDetails : IDisposable
{
    public int PagesCount { get; set; }
    public double Width { get; set; }
    public double Height { get; set; }
    public string[] m_arrPageLabels;



    public void Dispose()
    {
        m_pdfDocument?.Close();   // or .Dispose(); Close() is enough
        m_pdfDocument = null;
        GC.SuppressFinalize(this);
    }

    ~PDFDetails() => Dispose();   // optional safety net

    protected PdfDocument m_pdfDocument;

    public PDFDetails() { }

    // iText 9 version – works on iText Core (iText 7/8/9 API family)
    public static string[] GetPageLabels(iText.Kernel.Pdf.PdfDocument pdfDoc)
    {
        int n = pdfDoc.GetNumberOfPages();

        // /PageLabels lives in the catalog as a number-tree
        PdfDictionary catalog = pdfDoc.GetCatalog().GetPdfObject();
        PdfDictionary labels = catalog.GetAsDictionary(new PdfName("PageLabels"));
        if (labels == null) return null;

        // Flatten the number tree to a simple map<int, PdfDictionary>
        var numberTree = new Dictionary<int, PdfDictionary>();
        ReadNumberTree(labels, numberTree);

        string[] labelStrings = new string[n];

        int pagecount = 1;
        string prefix = "";
        char type = 'D'; // D=Decimal, R=UpperRoman, r=LowerRoman, A=UpperAlpha, a=LowerAlpha, e=empty

        for (int i = 0; i < n; i++)
        {
            if (numberTree.TryGetValue(i, out PdfDictionary d))
            {
                // Start number (spec uses /St)
                var stNum = d.GetAsNumber(new PdfName("St"));
                pagecount = stNum != null ? stNum.IntValue() : 1;

                // Prefix (/P)
                var pfx = d.GetAsString(PdfName.P);
                prefix = pfx != null ? pfx.ToUnicodeString() : "";

                // Style (/S) one of D, R, r, A, a (if absent -> no numeric part)
                var sName = d.GetAsName(PdfName.S);
                type = sName == null ? 'e' : sName.GetValue()[0];
            }

            switch (type)
            {
                default: // 'D'
                    labelStrings[i] = prefix + pagecount.ToString();
                    break;
                case 'R':
                    labelStrings[i] = prefix + ToRoman(pagecount, upper: true);
                    break;
                case 'r':
                    labelStrings[i] = prefix + ToRoman(pagecount, upper: false);
                    break;
                case 'A':
                    labelStrings[i] = prefix + ToAlpha(pagecount, upper: true);
                    break;
                case 'a':
                    labelStrings[i] = prefix + ToAlpha(pagecount, upper: false);
                    break;
                case 'e': // no numeric part
                    labelStrings[i] = prefix;
                    break;
            }
            pagecount++;
        }

        return labelStrings;
    }

    // Recursively read a number tree node: supports /Nums and /Kids
    private static void ReadNumberTree(PdfDictionary node, IDictionary<int, PdfDictionary> map)
    {
        var nums = node.GetAsArray(new PdfName("Nums"));
        if (nums != null)
        {
            for (int i = 0; i < nums.Size(); i += 2)
            {
                int pageIndex = nums.GetAsNumber(i).IntValue();
                PdfDictionary d = nums.GetAsDictionary(i + 1);
                if (d != null) map[pageIndex] = d;
            }
        }

        var kids = node.GetAsArray(PdfName.Kids);
        if (kids != null)
        {
            for (int i = 0; i < kids.Size(); i++)
            {
                PdfDictionary kid = kids.GetAsDictionary(i);
                if (kid != null) ReadNumberTree(kid, map);
            }
        }
    }

    // Roman numerals up to 3999 (good enough for page labels)
    private static string ToRoman(int number, bool upper)
    {
        if (number <= 0) return "";
        var vals = new[] { 1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1 };
        var syms = new[] { "M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I" };
        var s = new System.Text.StringBuilder();
        for (int i = 0; i < vals.Length && number > 0; i++)
        {
            while (number >= vals[i]) { number -= vals[i]; s.Append(syms[i]); }
        }
        var str = s.ToString();
        return upper ? str : str.ToLowerInvariant();
    }

    // Alphabetic sequence: 1->A, 26->Z, 27->AA, etc.
    private static string ToAlpha(int number, bool upper)
    {
        if (number <= 0) return "";
        var chars = new Stack<char>();
        int n = number;
        while (n > 0)
        {
            n--; // 1-based
            char c = (char)('A' + (n % 26));
            chars.Push(upper ? c : char.ToLowerInvariant(c));
            n /= 26;
        }
        return new string(chars.ToArray());
    }

    public PdfPage GetPage(int pageNum)
    {
        PdfPage oPdfPage = m_pdfDocument.GetPage(pageNum);
        return oPdfPage;
    }

    public void Init(string path)
    {
        try
        {
            m_pdfDocument = new PdfDocument(new PdfReader(path));
            PagesCount = m_pdfDocument.GetNumberOfPages();

            Rectangle size = m_pdfDocument.GetPage(1).GetPageSize();
            Width = size.GetWidth();
            Height = size.GetHeight();

            m_arrPageLabels = GetPageLabels(m_pdfDocument); // null if document has no /PageLabels
        }
        catch (Exception ex)
        {
            // replace with your logger
            Console.Error.WriteLine($"Cannot open PDF file {path}: {ex}");
        }
    }
}
