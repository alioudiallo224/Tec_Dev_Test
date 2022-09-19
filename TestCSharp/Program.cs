using System.Net;
using Npgsql;
using System.Data;

public class Program
{
    static void Main(string[] args)
    {
        List<string> listData = new List<string>();
        int i = 0;
        CreateTable();
        while (i < 3)
        {
            DateTime date = DateTime.Now.AddDays(-i);
            string day = string.Format("{0:00}", date.Day);
            string month = string.Format("{0:00}", date.Month);
            string year = string.Format("{0:0000}", date.Year);
            Console.WriteLine("Data for: " + date);
            listData = SplitCSV(day, month, year);
            DataValidation(listData, date);
            Console.WriteLine("Data Inserted into DataBase");
            Console.WriteLine("\n-----------------------------------------------------\n");
            i++;
        }
    }

    /*
     * Getting Data from url
     * @param url
     * @return Data from url
     */
    static string GetCSV(string url)
    {
        HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
        HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
        StreamReader sr = new StreamReader(resp.GetResponseStream());
        string data = sr.ReadToEnd();
        sr.Close();
        return data;
    }

    /*
     * Spliting CSV data from the url
     * @param the day, month and year of data to split
     * @return splitted data in a liste
     */
    static List<string> SplitCSV(string day, string month, string year)
    {
        Console.WriteLine("Downloading data");
        List<string> splitted = new List<string>();
        string fileList = GetCSV("https://twtransfer.energytransfer.com/ipost/TW/capacity/" +
            "operationally-available?f=csv&extension=csv&asset=TW&gasDay=" + month + "%2F" + day +
            "%2F" + year + "&cycle=5&searchType=NOM&searchString=&locType=ALL&locZone=ALL");
        string[] tempStr;

        tempStr = fileList.Split("Qty Reason\"");
        fileList = tempStr[1];
        tempStr = fileList.Split(',');
        foreach (string item in tempStr)
        {
            if (!string.IsNullOrWhiteSpace(item))
            {
                splitted.Add(item);
            }
        }
        Console.WriteLine("Data downloded");
        return splitted;
    }

    /*
     * Connect to the Server
     * @return connection
     */
    private static NpgsqlConnection GetConnection()
    {
        return new NpgsqlConnection(@"Server=localhost;Port=5433;User Id=postgres;Password=admin;Database=TecDevTest;");
    }

    /*
     * Create the table in the database
     */
    private static void CreateTable()
    {
        using (NpgsqlConnection conn = GetConnection())
        {
            conn.Open();
            if (conn.State == ConnectionState.Open)
            {
                using var cmd = new NpgsqlCommand();
                cmd.Connection = conn;

                cmd.CommandText = "DROP TABLE IF EXISTS energyTransfertTable";
                cmd.ExecuteNonQuery();

                cmd.CommandText = @"CREATE TABLE energyTransfertTable(
                                id SERIAL PRIMARY KEY,
                                loc VARCHAR(255) NOT NULL, 
                                locZn VARCHAR(255) NOT NULL,
                                locName VARCHAR(255) NOT NULL, 
                                locPurpDesc VARCHAR(255) NOT NULL, 
                                locQti VARCHAR(255) NOT NULL, 
                                flowIndicator VARCHAR(255) NOT NULL, 
                                dc VARCHAR(255) NOT NULL, 
                                opc VARCHAR(255) NOT NULL, 
                                tsq VARCHAR(255) NOT NULL,
                                oac VARCHAR(255) NOT NULL, 
                                it VARCHAR(255) NOT NULL, 
                                authOI VARCHAR(255) NOT NULL, 
                                nomCEI VARCHAR(255) NOT NULL, 
                                allQtyAv VARCHAR(255) NOT NULL, 
                                date DATE NOT NULL)";
                cmd.ExecuteNonQuery();

                Console.WriteLine("Table energyTransfertTable created");
            }
            else
            {
                Console.WriteLine("Connection felled");
            }
        }
    }

    /*
     * Insert data into database
     * @param Liste of data an date of the data
     */
    static void insertData(List<string> data, DateTime date_)
    {
        using (NpgsqlConnection conn = GetConnection())
        {
            conn.Open();
            if (conn.State == ConnectionState.Open)
            {
                var sql = "INSERT INTO energyTransfertTable " +
                                   "(loc, locZn,locName, " +
                                   "locPurpDesc, locQti, flowIndicator, dc, opc, tsq, oac, it, " +
                                   "authOI, nomCEI, allQtyAv, date) " +
                       "VALUES " +
                                   "(@loc, @locZn,@locName, " +
                                   "@locPurpDesc, @locQti, @flowIndicator, @dc, @opc, @tsq, @oac, @it, " +
                                   "@authOI, @nomCEI, @allQtyAv, @date)";
                using var cmd = new NpgsqlCommand(sql, conn);

                cmd.Parameters.AddWithValue("loc", data[0].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("locZn", data[1].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("locName", data[2].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("locPurpDesc", data[3].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("locQti", data[4].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("flowIndicator", data[5].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("dc", data[6].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("opc", data[7].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("tsq", data[8].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("oac", data[9].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("it", data[10].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("authOI", data[11].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("nomCEI", data[12].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("allQtyAv", data[13].Trim(new Char[] { '"' }));
                cmd.Parameters.AddWithValue("date", date_);
                cmd.Prepare();
                cmd.ExecuteNonQuery();
            }
            else
            {
                Console.WriteLine("Connection felled");
            }
        }
    }

    /*
     * Validate data
     * @param list of data, date of the data
     */
    static void DataValidation(List<string> listData, DateTime date)
    {
        List<string> subList = new List<string>();
        for (int i = 0; i < listData.Count-1; i+=14)
        {
            for (int j = 0; j < 14; j++)
            {
                subList.Add(listData[i+j]);

            }
            bool validation = DataValidation2(subList);
            if (validation) insertData(subList, date);
            subList.Clear();
        }        
    }

    /*
     * Validate data
     * @param list of data
     * @boolan: true if valide, false if not
     */
    static bool DataValidation2(List<string> subList)
    {
        if (!validateLocPurp(subList[3])) return false;
        if (!validateLocQI(subList[4])) return false;
        if (!validateFlowInd(subList[5])) return false;
        if (!validateITIndicator(subList[10])) return false;
        if (!validateAuthOverrun(subList[11])) return false;
        if (!validateCapExceed(subList[12])) return false;
        if (!validateAllQtyAv(subList[13])) return false;
        return true;
    }

    /*
     * Print custumized message for each validation error or retur true if valide
     * @param two valide strings, the string to check and error mesage
     * @return true if valide
     */
    static bool messageErreur(string s1, string s2, string s3, string message)
    {
        s3 = s3.Trim(new Char[] { '"' });
        bool result = (s3.Equals(s1) || s3.Equals(s2));
        if (!result)
        {
            Console.WriteLine(s3 + "invalide for " + message);
        }
        return result;
    }

    /*
     * Validate Location Purpose Description
     * @param Location Purpose Description
     * @retur true if valide or false if not
     */
    static bool validateLocPurp(string locPurp)
    {
        string valideLocPurp1 = "M2";
        string valideLocPurp2 = "MQ";
        string message = "Location Purpose Description";
        return messageErreur(valideLocPurp1, valideLocPurp2, locPurp.Trim(), message);
    }

    /*
     * Validate Location/Quantity Type Indicator
     * @param Location/Quantity Type Indicator
     * @retur true if valide or false if not
     */
    static bool validateLocQI(string locQI)
    {
        string valideLocQI1 = "RPQ";
        string valideLocQI2 = "DPQ";
        string message = "Location/Quantity Type Indicator";
        return messageErreur(valideLocQI1, valideLocQI2, locQI, message);
    }

    /*
     * Validate Flow Indicator
     * @param Flow Indicator
     * @retur true if valide or false if not
     */
    static bool validateFlowInd(string flowInd)
    {
        string valideFlowInd1 = "R";
        string valideFlowInd2 = "D";
        string message = "Flow Indicator";
        return messageErreur(valideFlowInd1, valideFlowInd2, flowInd, message);
    }

    /*
     * Validate IT Indicator
     * @param IT Indicator
     * @retur true if valide or false if not
     */
    static bool validateITIndicator(string iTIndicateur)
    {
        string valideITIndicateur1 = "Y";
        string valideITIndicateur2 = "N";
        string message = "IT Indicator";
        return messageErreur(valideITIndicateur1, valideITIndicateur2, iTIndicateur, message);
    }

    /*
     * Validate Authorized Overrun Indicator
     * @param Authorized Overrun Indicator
     * @retur true if valide or false if not
     */
    static bool validateAuthOverrun(string authOverrun)
    {
        string valideAuthOverrun1 = "Y";
        string valideAuthOverrun2 = "N";
        string message = "Authorized Overrun Indicator";
        return messageErreur(valideAuthOverrun1, valideAuthOverrun2, authOverrun, message);
    }

    /*
     * Nomination Capacity Exceeded Indicator
     * @param Nomination Capacity Exceeded Indicator
     * @retur true if valide or false if not
     */
    static bool validateCapExceed(string capExceed)
    {
        string valideCapExceed1 = "Y";
        string valideCapExceed2 = "N";
        string message = "Nomination Capacity Exceeded Indicator";
        return messageErreur(valideCapExceed1, valideCapExceed2, capExceed, message);
    }

    /*
     * Validate All Quantities Available Indicator/Quantity Not Availaible Reason
     * @param All Quantities Available Indicator/Quantity Not Availaible Reason
     * @retur true if valide or false if not
     */
    static bool validateAllQtyAv(string allQtyAv)
    {
        string valideAllQtyAv1 = "Y";
        string valideAllQtyAv2 = "N";
        string message = "All Quantities Available Indicator/Quantity Not Availaible Reason";
        return messageErreur(valideAllQtyAv1, valideAllQtyAv2, allQtyAv, message);
    }

}