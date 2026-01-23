using Amazon.Util.Internal;
using Core;
using Core.Models;
using DCCore.Common;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace core.Models.Convertor
{
    public class SearchHighlight
    {
        static public void deleteDocument(string sDocumentID)
        {

            /*

            //where publication_id = @publicationId
            var connectionString = ConfigurationManager.ConnectionStrings["DBConnectionString"].ConnectionString;
            using (var connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                string query = "DELETE FROM pagewords WHERE docid = @docid";
                MySqlCommand cmd = null;
                cmd = new MySqlCommand(query, connection);
                cmd.Parameters.AddWithValue("@docid", sDocumentID);
                cmd.ExecuteNonQuery();
            }
            */

        }

        public string getSearchHighlightCoordinates(string sDocumentID, string sLeftPage, string sRightPage,
            string sSearchTerm, string sZoomPage, int nHitOrder)
        {
            /*
            int nZoomPage = System.Convert.ToInt32(sZoomPage);
            string sResult = "";
            var connectionString = ConfigurationManager.ConnectionStrings["DBConnectionString"].ConnectionString;
            using (var connection = new MySqlConnection(connectionString))
            {
                connection.Open();
                MySqlCommand cmd = new MySqlCommand("SELECT * from pagewords WHERE docid = @docid and (pagenumber= @leftpage or pagenumber= @rightpage)", connection);
                cmd.Parameters.AddWithValue("@docid", sDocumentID);
                cmd.Parameters.AddWithValue("@leftpage", sLeftPage);
                cmd.Parameters.AddWithValue("@rightpage", sRightPage);
                ArrayList arrResults = new ArrayList();
                using (MySqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        dynamic pagewords2 = new ExpandoObject();
                        pagewords2.id = reader["id"].ToString();
                        pagewords2.searchterm = sSearchTerm;
                        //pagewords.pagetext = reader["pagetext"].ToString();
                        pagewords2.wordscoordinates = reader["wordscoordinates"].ToString();
                        pagewords2.pagenumber = reader["pagenumber"].ToString();
                        pagewords2.docid = reader["docid"].ToString();
                        arrResults.Add(pagewords2);
                    }
                }
                string DefaultIndexName = SettingsManager.Instance.getStringValue("esdefaultindex");

                ElasticSearchEngine oElasticSearchEngine = new ElasticSearchEngine();

                document oDocument = DocumentUtil.getDocumentById(sDocumentID);
                string sPublicationIndex = oDocument.Publication.searchindexname;
                if (string.IsNullOrEmpty(sPublicationIndex) == false)
                {
                    //Elastic support only lower case index.
                    sPublicationIndex = sPublicationIndex.ToLower();
                }
                string sIndexName = string.IsNullOrEmpty(sPublicationIndex) ? DefaultIndexName : sPublicationIndex;
                oElasticSearchEngine.IndexName = sIndexName;
                SearchRequest oSearchRequest = new SearchRequest(sDocumentID, sSearchTerm, Constants.SearchType.DOCUMENT,
                    100, 0, Constants.SortBy.PageNumber, false);
                ArrayList arrHitsList = oElasticSearchEngine.searchPage(oSearchRequest, System.Convert.ToInt32(sLeftPage), System.Convert.ToInt32(sRightPage), false);
                int nHitOnPageIndex = 0;
                for (int i = 0; i < arrHitsList.Count; i++)
                {

                    dynamic oHit = arrHitsList[i];
                    int nPageNumber = oHit.pagenumber;
                    string sSearchTerm2 = oHit.searchterm;
                    int nWordsOffset = oHit.wordsoffset - 1; //Need the index not the offset
                    dynamic pagewords;

                    for (int j = 0; j < arrResults.Count; j++)
                    {
                        pagewords = arrResults[j];
                        if (pagewords.pagenumber == nPageNumber.ToString())
                        {
                            if (nPageNumber == nZoomPage)
                                nHitOnPageIndex++;
                            ArrayList arrWordsLocation = new ArrayList();
                            arrWordsLocation.Add(nWordsOffset);

                            int countWords = sSearchTerm2.Split().Length;
                            for (int x = 1; x < countWords; x++)
                                arrWordsLocation.Add(nWordsOffset + x);

                            ArrayList arrWordsCoordinates = getCoordinatesByWordIndex(arrWordsLocation,
                                    pagewords.wordscoordinates);
                            StringBuilder sb = new StringBuilder();
                            foreach (object obj in arrWordsCoordinates)
                            {
                                if (sb.Length > 0)
                                    sb.Append(" ");
                                sb.Append(obj);
                            }
                            if ((nPageNumber == nZoomPage && nHitOnPageIndex == nHitOrder))
                            {
                                pagewords.zoomlocation = sb.ToString();
                            }


                            if (((IDictionary<String, Object>)pagewords).Keys.Contains("highlightedwordscoordinates"))
                            {
                                pagewords.highlightedwordscoordinates += " ";
                                pagewords.highlightedwordscoordinates += sb.ToString();
                            }
                            else
                            {
                                pagewords.highlightedwordscoordinates = sb.ToString();
                            }
                            break;
                        }

                    }
                }


                if (nZoomPage != -1)
                {


                    arrHitsList = oElasticSearchEngine.searchPage(oSearchRequest, System.Convert.ToInt32(sLeftPage), System.Convert.ToInt32(sRightPage), true);
                    nHitOnPageIndex = 0;
                    for (int i = 0; i < arrHitsList.Count; i++)
                    {

                        dynamic oHit = arrHitsList[i];
                        int nPageNumber = oHit.pagenumber;
                        string sSearchTerm2 = oHit.searchterm;
                        int nWordsOffset = oHit.wordsoffset - 1; //Need the index not the offset
                        dynamic pagewords;

                        for (int j = 0; j < arrResults.Count; j++)
                        {
                            pagewords = arrResults[j];
                            if (pagewords.pagenumber == nPageNumber.ToString())
                            {
                                if (nPageNumber == nZoomPage)
                                    nHitOnPageIndex++;
                                ArrayList arrWordsLocation = new ArrayList();
                                arrWordsLocation.Add(nWordsOffset);

                                int countWords = sSearchTerm2.Split().Length;
                                for (int x = 1; x < countWords; x++)
                                    arrWordsLocation.Add(nWordsOffset + x);

                                ArrayList arrWordsCoordinates = getCoordinatesByWordIndex(arrWordsLocation,
                                        pagewords.wordscoordinates);
                                StringBuilder sb = new StringBuilder();
                                foreach (object obj in arrWordsCoordinates)
                                {
                                    if (sb.Length > 0)
                                        sb.Append(" ");
                                    sb.Append(obj);
                                }
                                if ((nPageNumber == nZoomPage && nHitOnPageIndex == nHitOrder))
                                {
                                    pagewords.zoomlocation = sb.ToString();
                                    break;
                                }


                            }

                        }
                    }
                }
                for (int i = 0; i < arrResults.Count; i++)
                {
                    dynamic pagewords = arrResults[i];
                    ((IDictionary<String, Object>)pagewords).Remove("pagetext");
                    ((IDictionary<String, Object>)pagewords).Remove("wordscoordinates");
                }
                sResult = JsonConvert.SerializeObject(arrResults);

            }
            return sResult;
            */
            return "";
        }




        protected ArrayList getHighlightWordsLocation(string sPageText, string sSearchTerm)
        {
            ArrayList arrWordsIndexs = new ArrayList();
            sSearchTerm = sSearchTerm.Trim();
            bool bFound = true;
            string sText = sPageText;
            int nPrevIndex = 0;
            int nTemp = 0;
            while (bFound)
            {
                int nIndex = sText.IndexOf(sSearchTerm);
                if (nIndex == -1)
                    bFound = false;
                else
                {
                    string sPageMatchLocation = sText.Substring(0, nIndex);
                    int nFirstWordIndex = sPageMatchLocation.Split(' ').Length - 1;
                    int nNumberOfWords = sSearchTerm.Split(' ').Length;
                    for (int i = 0; i < nNumberOfWords; i++)
                    {
                        arrWordsIndexs.Add(nPrevIndex + nFirstWordIndex + i);
                        nTemp = nPrevIndex + nFirstWordIndex + i;
                    }
                    nPrevIndex = nTemp;
                    sText = sText.Substring(nIndex + sSearchTerm.Length);
                }
            }
            return arrWordsIndexs;
        }

        protected ArrayList getCoordinatesByWordIndex(ArrayList arrWordsIndexs, string sPageCoordinates)
        {
            ArrayList arrWordsCoordinates = new ArrayList();
            string[] arrPageCordinates = sPageCoordinates.Split(' ');
            foreach (int wordIndex in arrWordsIndexs)
            {
                for (int i = 0; i < 4; i++)
                {
                    arrWordsCoordinates.Add(arrPageCordinates[wordIndex * 4 + i]);
                }

            }
            return arrWordsCoordinates;
        }


    }
}
