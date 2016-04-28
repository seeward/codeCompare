using System;
using System.Text;
using System.Data;
using ICS.Database.TableObjects.JDE;
using ICS.Database.TableObjects.RFS;
using ICS.Display;
using ICS.ERPI.JDE;
using ICS.Utilities;
using ICS.Workflow.Base.WorkflowDesign.Components;


namespace ICS.Base.Wireless.JDE.Workflow.Picking
{
    public partial class JDESalesOrderPicking : ICS.Workflow.Base.WorkflowDesign.Components.RFSWorkflowDesigner
    {
        #region enums
        /// <summary>
        /// Enum holds values for different ways of traversing warehouse during picking.
        /// </summary>
        public enum SOPickAlgorithm : int
        {
            NoOptimization = 0,
            /// <summary>
            /// pick-to-clear - pick the smallest qty first
            /// </summary>
            PickToClear = 1,
            /// <summary>
            /// efficiency - pick from as few locations as possible
            /// </summary>
            Efficiency = 2
        }
        #endregion
        #region Class Variables
        #region Tables
        F0006 _branchTable;
        F4211 _salesOrderTable;
        F4100 _locationTable;
        F4101 _itemMasterTable;
        F4102 _itemBranchTable;
        F41001 _warehouseConstantsTable;
        F41002 _uomConversionTable;
        F41003 _uomConversionTable2;
        F41021 _itemLocationTable;
        F4108 _lotTable;
        F0101 _addressBookTable;
        F59RFCTL _rfsCartonTable;
        F4620 _jdeCartonTable;
        RFS_PrinterDefinition _printersTable;
        DataTable _skippedRecords;
        DataTable _cartonRecords;
        DataTable _cachedRecords;
        RFS_PickReservations _pickReservations;
        RFS_Vocabulary _vocabTable;
        #endregion
        #region Workflow Options
        string[] _allowedStatus;
        string[] _allowedDocTypes;
        string[] _allowedHoldCodes;
        string _transferP4113Version = string.Empty;
        string _defaultUOMWFO = string.Empty;
        string _dynamicPick = "1";
        bool _cartonEnabled = false;
        bool _allowOverrideHardCommits = false;
        string _orderBy = string.Empty;
        string _pickManagerMode = string.Empty;
        bool _allowLargerUOM = false;
        bool _forceBranch = false;
        bool _FEFOActive = false;
        SOPickAlgorithm _pickAlgorithm = SOPickAlgorithm.Efficiency;
        bool _defaultCNID = false;
        bool _reservePicks = false;
        int _maxLabels = 0;
        string _emailZeroPick = string.Empty;
        string _emailE1Error = string.Empty;
        bool _defaultVerifyItem = false;
        private bool _defaultPalletID = false;
        private string _serialNumberType = string.Empty;
        #endregion
        #region Collected Data
        string _printerAddress = string.Empty;
        string _primaryItem = string.Empty;
        double _shortItem = double.MinValue;
        string _defaultUOM = string.Empty;
        bool _recordLocked = false;
        double _conversionFactor = double.MinValue;
        double _quantity = double.MinValue;
        double _orderUomtoPrimary = double.MinValue;
        bool _forceLine = false;
        string _palletType = string.Empty;
        private double _previousOrder = 0;
        string _shippingPrinterAddress = string.Empty;
        string _shipLabelReturnScreen = string.Empty;
        bool _isMixed = false;
        #endregion
        #region Expected Data
        double _expectedShortItem = double.MinValue;
        double _expectedQuantityInPrimaryUOM = double.MinValue;
        string _expectedHCLocn = string.Empty;
        string _expectedHCLot = string.Empty;
        string _expectedLocn = string.Empty;
        string _expectedLot = string.Empty;
        bool _isHardCommitted = false;
        bool _isFinished = false;
        #endregion
        #region ERPI
        ICS.ERPI.JDE.SalesOrderPick _pickTx = new SalesOrderPick();
        #endregion
        #endregion
        #region Constructors
        public JDESalesOrderPicking(UserSession userSession) :
            base(ref userSession)
        {
            InitializeComponent();
            Transactions.ERP = "PeopleSoft";
        }
        public JDESalesOrderPicking()
            : base()
        {
            InitializeComponent();
        }
        #endregion
        #region Initialize
        protected override void Initialize()
        {
            #region Tables
            _branchTable = new F0006(UserSession.DataAccess, "");
            _salesOrderTable = new F4211(UserSession.DataAccess, "");
            _printersTable = new RFS_PrinterDefinition(UserSession.DataAccess, "");
            _locationTable = new F4100(UserSession.DataAccess, "");
            _itemMasterTable = new F4101(UserSession.DataAccess, "");
            _itemBranchTable = new F4102(UserSession.DataAccess, "");
            _warehouseConstantsTable = new F41001(UserSession.DataAccess, "");
            _uomConversionTable = new F41002(UserSession.DataAccess, "");
            _uomConversionTable2 = new F41003(UserSession.DataAccess, "");
            _itemLocationTable = new F41021(UserSession.DataAccess, "");
            _lotTable = new F4108(UserSession.DataAccess, "");
            _jdeCartonTable = new F4620(UserSession.DataAccess, "");
            _rfsCartonTable = new F59RFCTL(UserSession.DataAccess, "");
            _pickReservations = new RFS_PickReservations(UserSession.DataAccess, "");
            _vocabTable = new RFS_Vocabulary(UserSession.DataAccess, "");
            _addressBookTable = new F0101(UserSession.DataAccess, "");

            #endregion
            #region WorkflowOptions
            _allowedStatus = WFOptions.RetrieveOptionValue("IncludeStatus").Split(new char[] { ' ' });
            _allowedDocTypes = WFOptions.RetrieveOptionValue("AllowedDocumentTypes").Split(new char[] { ' ' });
            _allowedHoldCodes = WFOptions.RetrieveOptionValue("AllowedLotHoldCodes").Split(new char[] { ' ' });
            _transferP4113Version = WFOptions.RetrieveOptionValue("TransferP4113Version");
            _defaultUOMWFO = WFOptions.RetrieveOptionValue("DefaultUOM");
            _dynamicPick = WFOptions.RetrieveOptionValue("DynamicPick");
            _cartonEnabled = WFOptions.RetrieveOptionValue("CartonP4620Version").Trim().Length > 0;
            _allowOverrideHardCommits = WFOptions.GetYesNoWFOptionValue("AllowOverrideHardCommits");
            _orderBy = WFOptions.RetrieveOptionValue("OrderBy");
            _pickManagerMode = WFOptions.RetrieveOptionValue("PickManagerMode");
            _allowLargerUOM = WFOptions.GetYesNoWFOptionValue("AllowLargeUOM");
            _forceBranch = WFOptions.GetYesNoWFOptionValue("ForceBranch");
            try
            {
                _pickAlgorithm = (SOPickAlgorithm)WFOptions.GetInt32WFOptionValue("DynamicPickMode");
            }
            catch
            {
                _pickAlgorithm = SOPickAlgorithm.NoOptimization;
            }
            _FEFOActive = WFOptions.GetYesNoWFOptionValue("DynamicPickFEFO");
            _defaultCNID = WFOptions.GetYesNoWFOptionValue("DefaultCNID");
            _reservePicks = WFOptions.GetYesNoWFOptionValue("DynamicReservePicks");
            _maxLabels = int.Parse(WFOptions.RetrieveOptionValue("MaxLabels"));
            _emailZeroPick = WFOptions.RetrieveOptionValue("EmailZeroPick");
            _emailE1Error = WFOptions.RetrieveOptionValue("EmailE1Errors");
            _defaultVerifyItem = WFOptions.GetYesNoWFOptionValue("DefaultVerifyItem");
            _defaultPalletID = WFOptions.GetYesNoWFOptionValue("DefaultPalletID");
            _serialNumberType = WFOptions.RetrieveOptionValue("SerialNumberType");
            #endregion
            #region InMemoryTables
            createSkipDataSet();
            createCartonTable();
            #endregion
        }
        #endregion
        #region LineTable
        private void createCartonTable()
        {
            _cartonRecords = new DataTable("Cartons");
            _cartonRecords.Columns.Add("carton", typeof(string));
            _cartonRecords.Columns.Add("quantity", typeof(double));
            _cartonRecords.Columns.Add("cartonType", typeof(string));
            _cartonRecords.Columns.Add("location", typeof(string));
            _cartonRecords.Columns.Add("container", typeof(string));
            _cartonRecords.Columns.Add("lot", typeof(string));
            _cartonRecords.Columns.Add("palletId", typeof(string));
            _cartonRecords.Columns.Add("palletType", typeof(string));
            _cachedRecords = _cartonRecords.Clone();
        }

        private void addCarton(string carton, string cartonType)
        {
            DataRow dr = this._cartonRecords.NewRow();
            dr["palletId"] = _collectPallet.ScreenInput;
            dr["palletType"] = _palletType;
            dr["carton"] = carton;
            dr["quantity"] = _quantity;
            dr["cartonType"] = cartonType;
            dr["location"] = _collectFromLocation.ScreenInput;
            dr["container"] = _collectContainer.ScreenInput.Trim();
            dr["lot"] = _collectLot.ScreenInput.Trim();
            _cartonRecords.Rows.Add(dr);

            _quantity = 0;
        }
        #endregion
        #region Dispose
        public override void Dispose()
        {
            ScreenMap.ClearAllWorkflows();
            unlockAllRecords();
            _cartonRecords.Dispose();
            _skippedRecords.Dispose();
            deletePickReservation();
            base.Dispose();
        }
        #endregion
        #region Pick data selection and locking
        #region F4211Queries
        private void getPickingData()
        {
            if (_collectLine.ScreenInput.Trim().Length > 0)
            {
                callRandomLineQuery(_collectLine.ScreenInput.Trim());
                if (!removeF4211LocksAndSort())
                {
                    throw new RFS_Exception("RecordLocked");
                }
            }
            else if (_collectItem.ScreenInput.Trim().Length > 0)
            {
                callRandomItemQuery(_shortItem);
                if (!removeF4211LocksAndSort())
                {
                    throw new RFS_Exception("RecordsLocked");
                }
            }
            else if (int.Parse(_dynamicPick) >= 3)
            {
                callF4211BasicQuery(); // This Gets us the Demand for the Dynamic Pick.
                if (_salesOrderTable.Rows.Count == 0)
                {
                    decisionTable1();
                    throw new RFS_Exception("OrderComplete");
                }
                // We have our demand, Lets Marry it to the supply...
                mergeDynamicData();
                if (!removeF4211LocksAndSort())
                {
                    decisionTable1();
                    throw new RFS_Exception("AllLinesLocked");
                }
                logSDPTable();
            }
            else
            {
                callF4211BasicQuery();
                if (_salesOrderTable.Rows.Count == 0)
                {
                    decisionTable1();
                    throw new RFS_Exception("OrderComplete");
                }
                if (!removeF4211LocksAndSort())
                {
                    decisionTable1();
                    throw new RFS_Exception("AllLinesLocked");
                }
            }
            loadScreenMapsFromF4211();
        }

        private void getPickingDataForceLine(string line)
        {
            if (int.Parse(_dynamicPick) >= 3 && _collectLine.ScreenInput.Trim().Length == 0
                && _collectItem.ScreenInput.Trim().Length == 0)
            {
                callRandomLineQuery(line);
                // This Gets us the Demand for the Dynamic Pick.
                if (_salesOrderTable.Rows.Count == 0)
                {
                    decisionTable1();
                    throw new RFS_Exception("OrderComplete");
                }
                // We have our demand, Lets Marry it to the supply...
                mergeDynamicData();
                if (!removeF4211LocksAndSort())
                {
                    decisionTable1();
                    throw new RFS_Exception("AllLinesLocked");
                }
                logSDPTable();
            }
            else
            {
                callRandomLineQuery(line);
                if (_salesOrderTable.Rows.Count == 0)
                {
                    decisionTable1();
                    throw new RFS_Exception("OrderComplete");
                }
                if (!removeF4211LocksAndSort())
                {
                    decisionTable1();
                    throw new RFS_Exception("AllLinesLocked");
                }
            }
            loadScreenMapsFromF4211();
        }

        private void logSDPTable()
        {
            Log.Warn("SO Pick Sort = " + _salesOrderTable.ThisDataTable.DefaultView.Sort);
            Log.Warn("SO Pick Filter = " + _salesOrderTable.ThisDataTable.DefaultView.RowFilter);
            Log.Warn("SO Pick Dataset Loaded.  Row Count: " + _salesOrderTable.ThisDataTable.DefaultView.Count.ToString());


            System.IO.StringWriter sw = new System.IO.StringWriter();
            _salesOrderTable.ThisDataTable.DefaultView.ToTable().WriteXml(sw, XmlWriteMode.IgnoreSchema);
            Log.Warn(sw.ToString());

            sw.Flush();
            sw.Close();
        }

        #region Merge
        private void mergeDynamicData()
        {
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("QtyToPick"))
                _salesOrderTable.ThisDataTable.Columns.Add("QtyToPick", typeof(double));
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("Available"))
                _salesOrderTable.ThisDataTable.Columns.Add("Available", typeof(double));
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("IOMMEJ"))
                _salesOrderTable.ThisDataTable.Columns.Add("IOMMEJ", typeof(int));
            if (getSupplyForDynamicPick())
            {
                //go thru each line in demand and assign supply loc/ will have to split demand into multiple lines if need to satisfy
                //from multiple locations. sort the list by location sort code, location
                double currentItem = 0;
                //Order by Item for the Merge
                _salesOrderTable.ThisDataTable.DefaultView.Sort = "SDITM";
                foreach (DataRowView _line in _salesOrderTable.ThisDataTable.DefaultView)
                {
                    //"Added" rows are the ones that were split in the process of assigning supply
                    if (_line.Row.RowState == DataRowState.Added)
                        continue;

                    _salesOrderTable.CurrentRow = _line.Row;
                    convertRemainSOQuantityToPrimaryUOM(true);

                    //if sales order line is hard committed no need to fetch supply, move on to the next record
                    if (isHardCommited())
                    {
                        _salesOrderTable["QtyToPick"] = _expectedQuantityInPrimaryUOM;
                        _salesOrderTable["Available"] = _expectedQuantityInPrimaryUOM;
                        _salesOrderTable["IOMMEJ"] = 0;
                        continue;
                    }
                    if (!hasSupply(ref currentItem))
                    {
                        Log.Warn(String.Format("No supply found for line {0} item {1}", _salesOrderTable.SDLNID.ToString(), _salesOrderTable.SDLITM));
                        _salesOrderTable["QtyToPick"] = 0;
                        _salesOrderTable["Available"] = 0;
                        _salesOrderTable["IOMMEJ"] = 0;
                        continue;
                    }
                    mergeSupply(currentItem);
                }
                //filter out rows with zero qty to pick(either no inventory found or line is locked)
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter = "QtyToPick > 0";
            }
            else
            {
                foreach (DataRowView _line in _salesOrderTable.ThisDataTable.DefaultView)
                {
                    _salesOrderTable.CurrentRow = _line.Row;
                    _salesOrderTable["QtyToPick"] = 0;
                    _salesOrderTable["Available"] = 0;
                    _salesOrderTable["IOMMEJ"] = 0;
                }
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter = "QtyToPick > 0";
            }
        }

        private void mergeSupply(double currentItem)
        {
            int supplyIndex = 0;
            //loop thru supply lines until all qty on the sales order is satisfied or ran out of supply
            for (; _expectedQuantityInPrimaryUOM > 0 && supplyIndex < _itemLocationTable.ThisDataTable.DefaultView.Count; supplyIndex++)
            {
                _itemLocationTable.CurrentRow = _itemLocationTable.ThisDataTable.DefaultView[supplyIndex].Row;
                if (_itemLocationTable.Availability <= 0)
                    continue;

                #region special logic b\c FEFO takes precedence over efficiency
                if (_FEFOActive && _pickAlgorithm == SOPickAlgorithm.Efficiency)
                {
                    //check to see if can satisfy entire qty from record with oldest exp date. if yes, we're done, get out of supply loop
                    _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString() +
                    " AND AVAILABILITY >= " + _expectedQuantityInPrimaryUOM.ToString() + "AND IOMMEJ = " + _itemLocationTable["IOMMEJ"].ToString();
                    _itemLocationTable.ThisDataTable.DefaultView.Sort = "AVAILABILITY ASC, LILOCN";
                    if (_itemLocationTable.ThisDataTable.DefaultView.Count > 0)
                    {
                        for (int j = 0; j < _itemLocationTable.ThisDataTable.DefaultView.Count; j++)
                        {

                            _itemLocationTable.CurrentRow = _itemLocationTable.ThisDataTable.DefaultView[j].Row;

                            _salesOrderTable["QtyToPick"] = _expectedQuantityInPrimaryUOM;
                            _salesOrderTable["Available"] = _expectedQuantityInPrimaryUOM;
                            _salesOrderTable["IOMMEJ"] = int.Parse(_itemLocationTable["IOMMEJ"].ToString()); ;
                            setLocationMasterFields();
                            _salesOrderTable.SDLOCN = _itemLocationTable.LILOCN;
                            _salesOrderTable.SDLOTN = _itemLocationTable.LILOTN;
                            _expectedQuantityInPrimaryUOM = 0;
                            break;
                        }
                    }
                    //if couldn't find a line to satisfy entire qty from oldest exp date record, still need to use oldest record,
                    //so bring back original view filters and continue processing normally
                    if (_expectedQuantityInPrimaryUOM > 0)
                    {
                        _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString();
                        _itemLocationTable.ThisDataTable.DefaultView.Sort = "IOMMEJ, AVAILABILITY DESC, LILOCN";

                        _itemLocationTable.CurrentRow = _itemLocationTable.ThisDataTable.DefaultView[supplyIndex].Row;
                    }
                }
                #endregion
                if (_expectedQuantityInPrimaryUOM > 0)
                {
                    if (_expectedQuantityInPrimaryUOM <= _itemLocationTable.Availability)
                    {
                        _salesOrderTable["QtyToPick"] = _expectedQuantityInPrimaryUOM;
                        _salesOrderTable["Available"] = _itemLocationTable.Availability;
                        _salesOrderTable["IOMMEJ"] = int.Parse(_itemLocationTable["IOMMEJ"].ToString());
                        setLocationMasterFields();
                        _salesOrderTable.SDLOCN = _itemLocationTable.LILOCN;
                        _salesOrderTable.SDLOTN = _itemLocationTable.LILOTN;
                        _expectedQuantityInPrimaryUOM = 0;
                    }
                    else
                    {
                        _expectedQuantityInPrimaryUOM -= _itemLocationTable.Availability;
                        DataRow r = _salesOrderTable.ThisDataTable.NewRow();
                        r.ItemArray = _salesOrderTable.CurrentRow.ItemArray.Clone() as object[];
                        r["IOMMEJ"] = int.Parse(_itemLocationTable["IOMMEJ"].ToString());
                        r["Available"] = _itemLocationTable.Availability;
                        r["QtyToPick"] = _itemLocationTable.Availability;
                        //r["SDSOQS"] = convertAvailableToSalesOrderUOM();
                        r["SDLOCN"] = _itemLocationTable.LILOCN;
                        r["SDLOTN"] = _itemLocationTable.LILOTN;
                        foreach (string column in _locationTable.ColumnsForSelect.Split(new char[] { ',' }))
                        {
                            r[column.Trim()] = _itemLocationTable[column.Trim()];
                        }

                        //create new row for the part of qty satisfied by supply
                        //remaining qty will stay on main sales order record
                        _salesOrderTable.ThisDataTable.Rows.Add(r);
                    }
                }
            }


            if (_expectedQuantityInPrimaryUOM > 0)
            {
                //didn't have enough inventory to satisfy entire sales order qty, do not include remainder in suggested picking list
                _salesOrderTable["QtyToPick"] = 0;
                _salesOrderTable["Available"] = 0;
            }
        }

        private void setLocationMasterFields()
        {

            foreach (string column in _locationTable.ColumnsForSelect.Split(new char[] { ',' }))
            {
                _salesOrderTable[column.Trim()] = _itemLocationTable[column.Trim()];
            }
        }

        private bool hasSupply(ref double currentItem)
        {
            //only filter new supply if started new item, otherwise, exaust existing supply
            if (currentItem != _salesOrderTable.SDITM)
            {
                currentItem = _salesOrderTable.SDITM;
                _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString();
                switch (_pickAlgorithm)
                {
                    case SOPickAlgorithm.PickToClear:
                        _itemLocationTable.ThisDataTable.DefaultView.Sort =
                            (_FEFOActive ? "IOMMEJ, " : "") + "AVAILABILITY ASC, LILOCN";

                        break;
                    case SOPickAlgorithm.Efficiency:
                        if (_FEFOActive)
                        {
                            _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString();
                            _itemLocationTable.ThisDataTable.DefaultView.Sort = "IOMMEJ, AVAILABILITY DESC, LILOCN";

                        }
                        else
                        {
                            //satisfy from line from where there is just enough qty
                            _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString() +
                             " AND AVAILABILITY >= " + _expectedQuantityInPrimaryUOM.ToString();
                            _itemLocationTable.ThisDataTable.DefaultView.Sort = "AVAILABILITY ASC, LILOCN";

                            if (_itemLocationTable.ThisDataTable.DefaultView.Count == 0)
                            {
                                _itemLocationTable.ThisDataTable.DefaultView.RowFilter = "LIITM = " + currentItem.ToString();
                                _itemLocationTable.ThisDataTable.DefaultView.Sort = "AVAILABILITY DESC, LILOCN";
                            }
                        }
                        break;
                }
            }
            return _itemLocationTable.ThisDataTable.DefaultView.Count > 0;
        }
        #endregion

        #region convertRemainSOQuantityToPrimaryUOM
        public void convertRemainSOQuantityToPrimaryUOM(bool isLoading)
        {
            _shortItem = _salesOrderTable.SDITM;
            if (!isLoading && _salesOrderTable["QtyToPick"] != null && _salesOrderTable["QtyToPick"].ToString().Trim().Length > 0)
            {
                _expectedQuantityInPrimaryUOM = double.Parse(_salesOrderTable["QtyToPick"].ToString());
            }
            else
            {
                _orderUomtoPrimary = ConvertToUOMQty(_salesOrderTable.SDUOM, _salesOrderTable.CurrentRow["IMUOM1"].ToString(), 1);
                _expectedQuantityInPrimaryUOM = _orderUomtoPrimary * _salesOrderTable.SDSOQS;
                if (_forceLine)
                {
                    _expectedQuantityInPrimaryUOM -= ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput, _salesOrderTable.CurrentRow["IMUOM1"].ToString(), getPickedQty());
                }
            }
        }
        #endregion

        #region convertAvailableToSalesOrderUOM
        public double convertAvailableToSalesOrderUOM()
        {
            return _itemLocationTable.Availability / _orderUomtoPrimary;

        }
        #endregion

        #region getSupplyForDynamicPick
        private bool getSupplyForDynamicPick()
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    _itemLocationTable.GetSupplyForSOPickingByOrder(_collectBranch.ScreenInput, double.Parse(_collectOrder.ScreenInput),
                            _collectOrderType.ScreenInput, _allowedStatus, _allowedHoldCodes, UserSession.UserId, _pickManagerMode);
                    break;
                case "2": // by PickSlip
                    _itemLocationTable.GetSupplyForSOPickingByPickSlip(_collectBranch.ScreenInput, double.Parse(_collectPickSlip.ScreenInput),
                            _allowedStatus, _allowedHoldCodes, UserSession.UserId, _pickManagerMode);
                    break;
                case "3": // by Shipment
                    _itemLocationTable.GetSupplyForSOPickingByShipment(_collectBranch.ScreenInput, double.Parse(_collectShipment.ScreenInput),
                            _allowedStatus, _allowedHoldCodes, UserSession.UserId, _pickManagerMode);
                    break;
            }
            removeF41021Reservations();
            removeCachedPicks();
            return (_itemLocationTable.Rows.Count > 0);
        }
        #endregion

        #region removeF41021Reservations
        private void removeF41021Reservations()
        {
            if (_pickReservations.FetchReservations(UserSession.EnvironmentId, _collectBranch.ScreenInput))
            {
                foreach (DataRow dr in _itemLocationTable.Rows)
                {
                    _itemLocationTable.CurrentRow = dr;
                    string filter = "ShortItem = " + _itemLocationTable.LIITM.ToString();
                    filter += " AND Location = '" + _itemLocationTable.LILOCN + "' ";
                    filter += " AND Lot = '" + _itemLocationTable.LILOTN + "' ";
                    if (UserSession.SessionID.Length > 36)
                    {
                        filter += " AND SessionID <> '" + UserSession.SessionID.Substring(0, 36) + "'";
                    }
                    else
                    {
                        filter += " AND SessionID <> '" + UserSession.SessionID + "'";
                    }
                    double reservedQty = 0;
                    try
                    {
                        reservedQty = double.Parse(_pickReservations.ThisDataTable.Compute("SUM(QuantityInPrimary)", filter).ToString());
                    }
                    catch
                    {
                    }
                    if (reservedQty > 0)
                    {
                        _itemLocationTable.LIPQOH = _itemLocationTable.LIPQOH - reservedQty;
                        _itemLocationTable.Availability = _itemLocationTable.Availability - reservedQty;
                    }
                }
            }
        }
        #endregion

        #region removeCachedPicks
        private void removeCachedPicks()
        {
            if (_forceLine)
            {
                foreach (DataRow dr in _itemLocationTable.Rows)
                {
                    _itemLocationTable.CurrentRow = dr;
                    string filter = " location = '" + _itemLocationTable.LILOCN + "' ";
                    filter += " AND lot = '" + _itemLocationTable.LILOTN.Trim() + "' ";
                    double pickedQty = 0;
                    try
                    {
                        pickedQty = double.Parse(_cartonRecords.Compute("SUM(quantity)", filter).ToString());
                    }
                    catch
                    {
                    }
                    if (pickedQty > 0)
                    {
                        pickedQty = ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput, _salesOrderTable.CurrentRow["IMUOM1"].ToString(), pickedQty);
                        _itemLocationTable.LIPQOH = _itemLocationTable.LIPQOH - pickedQty;
                        _itemLocationTable.Availability = _itemLocationTable.Availability - pickedQty;
                    }
                }
            }
        }
        #endregion

        #region removeF4211LocksAndSort
        private bool removeF4211LocksAndSort()
        {
            if (!_salesOrderTable.RemoveLockedRecords())
            {
                return false;
            }
            if (_salesOrderTable.ThisDataTable.DefaultView.Count == 0)
            {
                //Remove Filter becaseu all pickable product has been picked.
                Log.Warn("All pickable product has been picked");
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter = "";
            }
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("skip"))
                _salesOrderTable.ThisDataTable.Columns.Add("skip");

            if (_skippedRecords.Rows.Count > 0)
            {
                foreach (DataRow row in _salesOrderTable.Rows)
                {
                    _salesOrderTable.CurrentRow = row;
                    if (isF4211Skipped())
                    {
                        _salesOrderTable.CurrentRow["skip"] = 1;
                    }
                }
                if (areAllRowsSkipped())
                {
                    _skippedRecords.Clear();
                }
            }
            if (_orderBy.Trim().Length > 0)
            {
                _salesOrderTable.ThisDataTable.DefaultView.Sort = "skip, " + _orderBy;
            }
            else
            {
                _salesOrderTable.ThisDataTable.DefaultView.Sort = "skip, LMWSQQ, SDLOCN";
            }
            _salesOrderTable.CurrentRow = _salesOrderTable.ThisDataTable.DefaultView[0].Row;
            return true;
        }
        #endregion

        #region callF4211BasicQuery
        private bool callF4211BasicQuery()
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    return _salesOrderTable.ValidOrder(_collectBranch.ScreenInput, _collectOrder.ScreenInput, _collectOrderType.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
                case "2": // by PickSlip
                    return _salesOrderTable.ValidPickSlip(_collectBranch.ScreenInput, _collectPickSlip.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
                case "3": // by Shipment
                    return _salesOrderTable.ValidShipment(_collectBranch.ScreenInput, _collectShipment.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
            }
            return false;
        }
        #endregion

        #region callRandomLineQuery
        private void callRandomLineQuery(string line)
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    if (!_salesOrderTable.ValidOrderLine(_collectBranch.ScreenInput, _collectOrder.ScreenInput, _collectOrderType.ScreenInput, _allowedStatus, _allowedDocTypes, line, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidOrderLine(_collectBranch.ScreenInput, _collectOrder.ScreenInput, _collectOrderType.ScreenInput, null, _allowedDocTypes, line, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("InvalidLine");
                        }
                    }
                    break;
                case "2": // by PickSlip
                    if (!_salesOrderTable.ValidPickSlipLine(_collectBranch.ScreenInput, _collectPickSlip.ScreenInput, _allowedStatus, _allowedDocTypes, line, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidPickSlipLine(_collectBranch.ScreenInput, _collectPickSlip.ScreenInput, null, _allowedDocTypes, line, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("InvalidLine");
                        }
                    }
                    break;
                case "3": // by Shipment
                    if (!_salesOrderTable.ValidShipmentLine(_collectBranch.ScreenInput, _collectShipment.ScreenInput, _allowedStatus, _allowedDocTypes, line, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidShipmentLine(_collectBranch.ScreenInput, _collectShipment.ScreenInput, null, _allowedDocTypes, line, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("InvalidLine");
                        }
                    }
                    break;
            }
        }
        #endregion

        #region callRandomItemQuery
        private void callRandomItemQuery(double shortItem)
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    if (!_salesOrderTable.ValidOrderItem(_collectBranch.ScreenInput, _collectOrder.ScreenInput, _collectOrderType.ScreenInput, _allowedStatus, _allowedDocTypes, shortItem, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidOrderItem(_collectBranch.ScreenInput, _collectOrder.ScreenInput, _collectOrderType.ScreenInput, null, _allowedDocTypes, shortItem, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("ItemNotOnOrder");
                        }
                    }
                    break;
                case "2": // by PickSlip
                    if (!_salesOrderTable.ValidPickSlipItem(_collectBranch.ScreenInput, _collectPickSlip.ScreenInput, _allowedStatus, _allowedDocTypes, shortItem, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidPickSlipItem(_collectBranch.ScreenInput, _collectPickSlip.ScreenInput, null, _allowedDocTypes, shortItem, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("ItemNotOnPickSlip");
                        }
                    }
                    break;
                case "3": // by Shipment
                    if (!_salesOrderTable.ValidShipmentItem(_collectBranch.ScreenInput, _collectShipment.ScreenInput, _allowedStatus, _allowedDocTypes, shortItem, UserSession.UserId, _pickManagerMode))
                    {
                        if (_salesOrderTable.ValidShipmentItem(_collectBranch.ScreenInput, _collectShipment.ScreenInput, null, _allowedDocTypes, shortItem, UserSession.UserId, "0"))
                        {
                            throw new RFS_Exception("AtWrongStatus");
                        }
                        else
                        {
                            throw new RFS_Exception("ItemNotOnShipment");
                        }
                    }
                    break;
            }
        }
        #endregion
        #endregion
        #region beginPicking
        private void beginPicking()
        {
            _recordLocked = true;
            if (isSerialItem())
            {
                double qty = 0;
                //Spin through F4211 and Lock All Records for the item
                foreach (DataRow dr in _salesOrderTable.ThisDataTable.DefaultView.ToTable().Rows)
                {
                    _salesOrderTable.CurrentRow = dr;
                    if (_salesOrderTable.SDITM == _itemMasterTable.IMITM)
                    {
                        if ((_collectLine.ScreenInput.Trim().Length > 0) || (_collectItem.ScreenInput.Trim().Length > 0) || (int.Parse(_dynamicPick) < 3))
                        {
                            qty += double.Parse(_salesOrderTable.SDSOQS.ToString());
                        }
                        else
                        {
                            qty += double.Parse(_salesOrderTable["QtyToPick"].ToString());
                        }
                        _salesOrderTable.LockRecord(UserSession.EnvironmentId, UserSession.UserId, WorkflowID);
                        reservePick(_salesOrderTable.SDLOCN, _salesOrderTable.SDLOTN);
                    }
                }
                _expectedQuantityInPrimaryUOM = ConvertToUOMQty(_salesOrderTable.SDUOM, _itemMasterTable.IMUOM1, qty);
                ScreenMap.Add("RemainingQtyInPrimary", _expectedQuantityInPrimaryUOM.ToString());
                _salesOrderTable.CurrentRow = _salesOrderTable.ThisDataTable.DefaultView[0].Row;
                ScreenMap.Add("CommittedLot", _salesOrderTable.SDLOTN);
                Next(_collectSerial);
            }
            else
            {
                convertRemainSOQuantityToPrimaryUOM(false);
                // Lock the Current Record
                _salesOrderTable.LockRecord(UserSession.EnvironmentId, UserSession.UserId, WorkflowID);
                reservePick(_salesOrderTable.SDLOCN, _salesOrderTable.SDLOTN);
                Next(_collectFromLocation);
            }
        }
        #endregion
        #region reservePick
        private bool reservePick(string location, string lot)
        {
            if (_reservePicks)
            {
                deletePickReservation();
                _pickReservations.EnvironmentID = UserSession.EnvironmentId;
                if (UserSession.SessionID.Length > 36)
                {
                    _pickReservations.SessionID = UserSession.SessionID.Substring(0, 36);
                }
                else
                {
                    _pickReservations.SessionID = UserSession.SessionID;
                }
                _pickReservations.Branch = _salesOrderTable.SDMCU;
                _pickReservations.ShortItem = int.Parse(_salesOrderTable.SDITM.ToString());
                _pickReservations.Location = location;
                _pickReservations.Lot = lot;
                _pickReservations.UserID = UserSession.CurrentUser.LoginName;
                _pickReservations.QuantityInPrimary = _expectedQuantityInPrimaryUOM;
                _pickReservations.LockDate = DateTime.Now;
                return _pickReservations.Insert() > 0;
            }
            return true;
        }
        #endregion
        #region deletePickReservation
        private bool deletePickReservation()
        {
            if (_reservePicks)
            {
                _pickReservations.EnvironmentID = UserSession.EnvironmentId;
                if (UserSession.SessionID.Length > 36)
                {
                    _pickReservations.SessionID = UserSession.SessionID.Substring(0, 36);
                }
                else
                {
                    _pickReservations.SessionID = UserSession.SessionID;
                }
                return _pickReservations.Delete() > 0;
            }
            return true;
        }
        #endregion
        #region loadScreenMapsFromF4211
        private void loadScreenMapsFromF4211()
        {
            _collectSerial.ScreenInput = string.Empty;
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("QtyToPick"))
                _salesOrderTable.ThisDataTable.Columns.Add("QtyToPick", typeof(double));
            if (!_salesOrderTable.ThisDataTable.Columns.Contains("Available"))
                _salesOrderTable.ThisDataTable.Columns.Add("Available", typeof(double));
            if (_pickAlgorithm == SOPickAlgorithm.PickToClear)
            {
                checkPickToClearQty();
            }
            ScreenMap.Add("Order", _salesOrderTable.SDDOCO.ToString());
            ScreenMap.Add("OrderType", _salesOrderTable.SDDOCO.ToString());
            ScreenMap.Add("PickSlip", _salesOrderTable.SDPSN.ToString());
            ScreenMap.Add("Shipment", _salesOrderTable.SDSHPN.ToString());
            ScreenMap.Add("Line", _salesOrderTable.SDLNID.ToString());
            ScreenMap.Add("CommittedLot", _salesOrderTable.SDLOTN);
            _primaryItem = _itemMasterTable.RetrieveItem(_collectBranch.ScreenInput,
                _itemMasterTable.FetchPrimaryItemNumber(_salesOrderTable.SDITM, _warehouseConstantsTable._DesignatedPrimaryItemNumber),
                _warehouseConstantsTable._DesignatedPrimaryItemNumber.ToString());
            _shortItem = _salesOrderTable.SDITM;
            ScreenMap.Add("PrimaryItemNumber", _primaryItem);
            ScreenMap.Add("ItemDescription", _itemMasterTable.IMDSC1);
            _locationTable.ValidateLocation(_collectBranch.ScreenInput, _salesOrderTable.SDLOCN);
            ScreenMap.Add("Location", _locationTable.sscnlocn);
            if (_defaultUOMWFO == "S")
            {
                _defaultUOM = _salesOrderTable.SDUOM;
            }
            else
            {
                _defaultUOM = _itemMasterTable.DefaultUOM(_defaultUOMWFO);
            }
            convertRemainSOQuantityToPrimaryUOM(false);
            if (_defaultUOM != "")
            {
                double remainingQtyInDefaultUOM = ConvertToUOMQty(_itemMasterTable.IMUOM1, _defaultUOM, _expectedQuantityInPrimaryUOM);
                ScreenMap.Add("RemainingQtyInDefaultUOM", remainingQtyInDefaultUOM.ToString());
                ScreenMap.Add("DefaultUOM", _defaultUOM);
            }
            else
            {
                double remainingQtyInDefaultUOM = ConvertToUOMQty(_itemMasterTable.IMUOM1, _salesOrderTable.SDUOM, _expectedQuantityInPrimaryUOM);
                ScreenMap.Add("RemainingQtyInDefaultUOM", remainingQtyInDefaultUOM.ToString());
                ScreenMap.Add("DefaultUOM", _salesOrderTable.SDUOM);
            }
            ScreenMap.Add("RemainingQty", _salesOrderTable.SDSOQS.ToString());
            ScreenMap.Add("UOM", _salesOrderTable.SDUOM);
            ScreenMap.Add("PrimaryUOM", _itemMasterTable.IMUOM1);
            _itemBranchTable.ValidateItemBranch(_collectBranch.ScreenInput, _salesOrderTable.SDITM);

            ScreenMap.Add("RemainingQtyInPrimary", _expectedQuantityInPrimaryUOM.ToString());
            _expectedShortItem = _salesOrderTable.SDITM;
            _expectedLocn = _salesOrderTable.SDLOCN;
            _expectedLot = _salesOrderTable.SDLOTN;
            if (!this._allowOverrideHardCommits && isHardCommited())
            {
                _isHardCommitted = true;
                _expectedHCLocn = _expectedLocn;
                _expectedHCLot = _expectedLot;
            }
            else
            {
                _isHardCommitted = false;
                _expectedHCLocn = string.Empty;
                _expectedHCLot = string.Empty;
            }
        }

        private void checkPickToClearQty()
        {
            if (_collectLine.ScreenInput.Trim().Length > 0 || _collectItem.ScreenInput.Trim().Length > 0 || int.Parse(_dynamicPick) < 3)
            {
                return;
            }
            double remaining = double.Parse(_salesOrderTable["Available"].ToString()) - double.Parse(_salesOrderTable["QtyToPick"].ToString());
            if (remaining > 0)
            {
                bool suggestAvailable = false;
                DataRow current = _salesOrderTable.CurrentRow;
                double shortItem = _salesOrderTable.SDITM;
                int lotExp = int.Parse(_salesOrderTable["IOMMEJ"].ToString());
                foreach (DataRow dr in _salesOrderTable.ThisDataTable.DefaultView.ToTable().Rows)
                {
                    _salesOrderTable.CurrentRow = dr;
                    if (_salesOrderTable.SDITM == shortItem && int.Parse(_salesOrderTable["IOMMEJ"].ToString()) == lotExp
                        && (dr["SDLOCN"] != current["SDLOCN"] || dr["SDLOTN"] != current["SDLOTN"]))
                    {
                        if (remaining <= double.Parse(_salesOrderTable["QtyToPick"].ToString()))
                        {
                            suggestAvailable = true;
                            break;
                        }
                    }
                }
                _salesOrderTable.CurrentRow = current;
                if (suggestAvailable)
                {
                    _salesOrderTable["QtyToPick"] = double.Parse(_salesOrderTable["Available"].ToString());
                }
            }
        }

        #endregion
        #region Skip
        #region createSkipDataSet
        private void createSkipDataSet()
        {
            _skippedRecords = new DataTable("Skipped");
            _skippedRecords.Columns.Add("branch", typeof(string));
            _skippedRecords.Columns.Add("shortItem", typeof(int));
            _skippedRecords.Columns.Add("location", typeof(string));
            _skippedRecords.Columns.Add("lot", typeof(string));
            DataColumn[] keys = new DataColumn[4];
            keys[0] = _skippedRecords.Columns[0];
            keys[1] = _skippedRecords.Columns[1];
            keys[2] = _skippedRecords.Columns[2];
            keys[3] = _skippedRecords.Columns[3];
            _skippedRecords.PrimaryKey = keys; ;
        }
        #endregion

        #region areAllRowsSkipped
        private bool areAllRowsSkipped()
        {
            DataRow[] foundRows;
            if (((_collectLine.ScreenInput != null) && (_collectLine.ScreenInput.Trim().Length > 0))
            || ((_collectItem.ScreenInput != null) && (_collectItem.ScreenInput.Trim().Length > 0))
            || int.Parse(_dynamicPick) < 3)
            {
                foundRows = _salesOrderTable.ThisDataTable.Select("skip is null");
            }
            else
            {
                foundRows = _salesOrderTable.ThisDataTable.Select("skip is null and QtyToPick > 0");
            }

            return foundRows.Length == 0;
        }
        #endregion

        #region isF4211Skipped
        private bool isF4211Skipped()
        {
            return _skippedRecords.Rows.Contains(new object[] {_collectBranch.ScreenInput,
                                                               _salesOrderTable.SDITM, 
                                                               _salesOrderTable.SDLOCN, 
                                                               _salesOrderTable.SDLOTN});
        }
        #endregion

        #region skipPick
        private void skipPick()
        {
            unlockAllRecords();
            DataRow dr = _skippedRecords.NewRow();
            dr["location"] = _expectedLocn;
            dr["lot"] = _expectedLot;
            dr["shortItem"] = _expectedShortItem;
            dr["branch"] = _collectBranch.ScreenInput;
            _skippedRecords.Rows.Add(dr);
            if (_collectItem.ScreenInput.Trim().Length > 0 || _collectLine.ScreenInput.Trim().Length > 0)
            {
                RestartLoop(_collectLine);
            }
            else
            {
                getPickingData();
                beginPicking();
            }
        }
        #endregion
        #endregion
        #region removeNonHardCommitedLotLocn
        private void removeNonHardCommitedLotLocn()
        {
            if (_isHardCommitted && _expectedHCLot.Trim().Length > 0 && !_allowOverrideHardCommits)
            {
                int i = 0;
                DataRow currentRow;
                while (i < _itemLocationTable.Rows.Count)
                {
                    //set the current row.
                    currentRow = _itemLocationTable.Rows[i];
                    if (_itemLocationTable.Rows[i]["LILOTN"].ToString().Trim().ToUpper() != _expectedHCLot.Trim().ToUpper() ||
                        _itemLocationTable.Rows[i]["LILOCN"].ToString().Trim().ToUpper() != _expectedHCLocn.Trim().ToUpper())
                    {
                        this._itemLocationTable.Rows.RemoveAt(i);
                        this._itemLocationTable.UpdateCurrentRow();
                        i--;
                    } // end if
                    i++;
                }
                // CC023260 - The CurrentRow was always the last row in the Record set. It needs to be the first row.
                if (_itemLocationTable.Rows.Count > 0)
                {
                    _itemLocationTable.CurrentRow = _itemLocationTable.Rows[0];
                }
            }
        }
        #endregion
        #region unlockAllRecords
        private void unlockAllRecords()
        {
            if (_recordLocked)
            {
                try
                {
                    if (isSerialItem())
                    {
                        foreach (DataRow dr in _salesOrderTable.Rows)
                        {
                            _salesOrderTable.CurrentRow = dr;
                            if (_salesOrderTable.SDITM == _itemMasterTable.IMITM)
                            {
                                _salesOrderTable.UnLockRecord(UserSession.EnvironmentId);
                            }
                        }
                        _salesOrderTable.CurrentRow = _salesOrderTable.Rows[0];
                    }
                    else
                    {
                        _salesOrderTable.UnLockRecord(UserSession.EnvironmentId);
                    }

                }
                catch
                {
                }
                try
                {
                    deletePickReservation();
                }
                catch
                {
                }
            }
        }
        #endregion
        #endregion
        #region Data helpers
        private bool isSerialItem()
        {
            if ((_itemBranchTable.IBSRCE.Trim().Length > 0) &&
                   (Convert.ToInt16(_itemBranchTable.IBSRCE) > Convert.ToInt16(LotSerialDesignations.LotRequired)))
            {
                return (true);
            }
            else
            {
                return false;
            }
        }

        private bool isLotItem()
        {
            if ((_itemBranchTable.IBSRCE.Trim().Length > 0) &&
               ((Convert.ToInt16(_itemBranchTable.IBSRCE) >= Convert.ToInt16(LotSerialDesignations.LotOptional)) &&
               (Convert.ToInt16(_itemBranchTable.IBSRCE) <= Convert.ToInt16(LotSerialDesignations.LotRequired))))
            {
                return (true);
            }
            else
            {
                return false;
            }
        }

        private bool isHardCommited()
        {
            return (_salesOrderTable.SDCOMM == "H" || _salesOrderTable.SDCOMM == "C");
        }
        #endregion
        #region QtyHelpers
        private double ConvertToUOMQty(string fromUOM, string toUOM, double qty)
        {
            double factor = 0;
            if (fromUOM == toUOM)
            {
                factor = 1;
            }
            else
            {
                factor = _uomConversionTable.ConvertToPrimaryUOM(_collectBranch.ScreenInput,
                                                                 _shortItem,
                                                                 fromUOM,
                                                                 toUOM);
                if (factor == 0)
                    factor = _uomConversionTable2.ConvertToPrimaryUOM(fromUOM, toUOM);
            }
            return (factor * qty);
        }

        private double getPickedQty()
        {
            if (_cartonRecords.Rows.Count == 0)
            {
                return 0;
            }
            return Convert.ToDouble(_cartonRecords.Compute("SUM(quantity)", ""));
        }

        private double getItemLotQty(string location, string lot)
        {
            if (_cartonRecords.Rows.Count == 0)
            {
                return 0;
            }
            string filter = string.Format("location = '{0}' AND lot = '{1}'",
            location, lot);
            try
            {
                return Convert.ToDouble(_cartonRecords.Compute("SUM(quantity)", filter));
            }
            catch
            {
                return 0;
            }
        }
        private double getItemLotContainerQty(string location, string lot, string container)
        {
            if (_cartonRecords.Rows.Count == 0)
            {
                return 0;
            }
            string filter = string.Format("location = '{0}' AND lot = '{1}' AND container = '{2}'",
            location, lot, container);
            try
            {
                return Convert.ToDouble(_cartonRecords.Compute("SUM(quantity)", filter));
            }
            catch
            {
                return 0;
            }
        }

        private bool isProductAvailable()
        {
            _itemLocationTable.RetrieveSpecificRecord(_collectBranch.ScreenInput,
                                                      _shortItem,
                                                      _collectFromLocation.ScreenInput,
                                                      _collectLot.ScreenInput);
            double qtyAvail = 0;
            double cartonQty = getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput);

            switch (WFOptions.RetrieveOptionValue("AvailabilityCheck"))
            {
                case "A": // available quantity
                    qtyAvail = Convert.ToDouble(_itemLocationTable.Availability.ToString()) / _conversionFactor;
                    // for hard-committed items, we need to back out the transaction quantity since
                    // it is represented in the committed calcs
                    if (_isHardCommitted)
                    {
                        if ((_collectFromLocation.ScreenInput.Trim() == this._expectedHCLocn.Trim()) &&
                            (_collectLot.ScreenInput.Trim() == _expectedHCLot.Trim()))
                        {
                            qtyAvail += _itemLocationTable.LIPQOH / _conversionFactor;
                        } // end if
                    } // end if
                    break;
                case "O": // on hand quantity
                    qtyAvail = _itemLocationTable.LIPQOH / _conversionFactor;
                    break;
                case "I":
                    qtyAvail = _quantity + cartonQty;
                    break;
            } // end switch

            Log.WriteLine(5, "Available Quantity = " + qtyAvail.ToString());

            if (_quantity + cartonQty > qtyAvail)
            {
                return false;
            }
            return true;
        }

        private bool isOverPick()
        {
            if (_quantity > (_expectedQuantityInPrimaryUOM / _conversionFactor) - getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput))
            {
                return true;
            }
            return false;
        }

        private bool isShortPick()
        {
            if (_quantity < (_expectedQuantityInPrimaryUOM / _conversionFactor) - getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput))
            {
                return true;
            }
            return false;
        }

        private bool isE1ShortPick()
        {
            _orderUomtoPrimary = ConvertToUOMQty(_salesOrderTable.SDUOM, _salesOrderTable.CurrentRow["IMUOM1"].ToString(), 1);

            if (_quantity < (_orderUomtoPrimary * _salesOrderTable.SDSOQS / _conversionFactor) - getPickedQty())
            {
                return true;
            }
            return false;
        }

        #endregion
        #region E1 Calls
        private void zeroPick()
        {
            Transactions.Add(_pickTx);
            _pickTx.Initialize();
            setShipTo();
            setSoldTo();
            _collectFromLocation.ScreenInput = _salesOrderTable.SDLOCN;
            setFromLocation();
            setItem();
            _pickTx.OrderNumber = _salesOrderTable.SDDOCO.ToString();
            _pickTx.OrderType = _salesOrderTable.SDDCTO.Trim();
            _pickTx.OrderCompany = _salesOrderTable.SDKCOO.Trim();
            _pickTx.LineNumber = _salesOrderTable.SDLNID.ToString();
            _pickTx.BranchPlant = _salesOrderTable.SDMCU.Trim();
            _pickTx.Lot = _salesOrderTable.SDLOTN.Trim();
            _pickTx.Quantity = _salesOrderTable.SDSOQS.ToString();
            _pickTx.UOM = _salesOrderTable.SDUOM.Trim();
            _pickTx.RFUserID = UserSession.CurrentUser.LoginName;
            _pickTx.Mode = "Z";
            _pickTx.TransDate_YYYYMMDD = System.DateTime.Now.ToString("yyyyMMdd");
            _pickTx.P4205Version = string.Empty;
            _pickTx.ToLocation = string.Empty;
            _pickTx.P4113Version = string.Empty;
            _pickTx.SecondaryQuantity = string.Empty;
            _pickTx.SecondaryUOM = string.Empty;
            _pickTx.ToCartonID = string.Empty;
            _pickTx.CartonQuantity = string.Empty;
            _pickTx.CustomerCartonId = string.Empty;
            _pickTx.CartonCode = string.Empty;
            _pickTx.CartonP4620Version = string.Empty;
            _pickTx.UserInput01 = string.Empty;
            _pickTx.UserInput02 = string.Empty;
            _pickTx.UserInput03 = string.Empty;
            _pickTx.UserInput04 = string.Empty;
            _pickTx.UserInput05 = string.Empty;
            _pickTx.UserInput06 = string.Empty;
            _pickTx.UserInput07 = string.Empty;
            _pickTx.ContainerID = string.Empty;
            _pickTx.ZeroPickNextStatus = WFOptions.RetrieveOptionValue("ZeroPickNextStatus");

            Transactions.Send(WFOptions.RetrieveOptionValue("SyncTransaction"));
            SendZeroMail();
            DisplayMessages();
            deletePickReservation();
        }
        private void setSoldTo()
        {
            _pickTx.CustSoldTo = _salesOrderTable.SDAN8.ToString();
            _addressBookTable.RetrieveAddress(_salesOrderTable.SDAN8.ToString());
            _pickTx.CustSoldToDescr = _addressBookTable.ABALPH.Trim();
        }
        private void setShipTo()
        {
            _pickTx.CustShipTo = _salesOrderTable.SDSHAN.ToString();
            _addressBookTable.RetrieveAddress(_salesOrderTable.SDSHAN.ToString());
            _pickTx.CustShipToDescr = _addressBookTable.ABALPH.Trim();
        }
        private void setFromLocation()
        {
            _pickTx.Location = _collectFromLocation.ScreenInput; // Keep format the same as in E1 DB.
            if (_locationTable.ValidateLocation(_salesOrderTable.SDMCU.Trim(), _collectFromLocation.ScreenInput))
            {
                _pickTx.FromLocnPutZone = _locationTable.LMPZON.Trim();
                _pickTx.FromLocnPickZone = _locationTable.LMKZON.Trim();
                _pickTx.FromLocnReplZone = _locationTable.LMZONR.Trim();
                _pickTx.FromLocnDimGroup = _locationTable.LMSTY1.Trim();
            }
        }
        private void setCartonFromLocation(string fromLocation)
        {
            _pickTx.Location = fromLocation; // Keep format the same as in E1 DB.
            if (_locationTable.ValidateLocation(_salesOrderTable.SDMCU.Trim(), fromLocation))
            {
                _pickTx.FromLocnPutZone = _locationTable.LMPZON.Trim();
                _pickTx.FromLocnPickZone = _locationTable.LMKZON.Trim();
                _pickTx.FromLocnReplZone = _locationTable.LMZONR.Trim();
                _pickTx.FromLocnDimGroup = _locationTable.LMSTY1.Trim();
            }
        }
        private void setToLocation()
        {
            _pickTx.ToLocation = _collectTransferLocation.ScreenInput; // Keep format the same as in E1 DB.
            if (_locationTable.ValidateLocation(_salesOrderTable.SDMCU.Trim(), _collectTransferLocation.ScreenInput))
            {
                _pickTx.ToLocnPutZone = _locationTable.LMPZON.Trim();
                _pickTx.ToLocnPickZone = _locationTable.LMKZON.Trim();
                _pickTx.ToLocnReplZone = _locationTable.LMZONR.Trim();
                _pickTx.ToLocnDimGroup = _locationTable.LMSTY1.Trim();
            }
        }
        private void setItem()
        {
            _pickTx.PrimaryItemNumber = _primaryItem.Trim();
            _pickTx.SecondItemNumber = _itemMasterTable.IMLITM.Trim();
            _pickTx.ShortItemNumber = _itemMasterTable.IMITM.ToString();
            _pickTx.ThirdItemNumber = _itemMasterTable.IMAITM.Trim();
            _pickTx.ItemDescription1 = _itemMasterTable.IMDSC1.Trim();
            _pickTx.ItemDescription2 = _itemMasterTable.IMDSC2.Trim();
            _itemBranchTable.ValidateItemBranch(_salesOrderTable.SDMCU.Trim(), _itemMasterTable.IMITM);
            _pickTx.ItemDimGroup = _itemBranchTable.IBPRP6.Trim();
            _pickTx.ItemLotProcType = _itemBranchTable.IBSRCE;
        }
        private void SendToERP()
        {
            Transactions.Add(_pickTx);
            _pickTx.Initialize();
            setSoldTo();
            setShipTo();
            setFromLocation();
            setToLocation();
            setItem();
            _pickTx.OrderNumber = _salesOrderTable.SDDOCO.ToString();
            _pickTx.OrderType = _salesOrderTable.SDDCTO.Trim();
            _pickTx.OrderCompany = _salesOrderTable.SDKCOO.Trim();
            _pickTx.LineNumber = _salesOrderTable.SDLNID.ToString();
            _pickTx.BranchPlant = _salesOrderTable.SDMCU.Trim();
            _pickTx.Lot = _collectLot.ScreenInput.Trim();
            _pickTx.Quantity = ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput,
                                               _salesOrderTable.SDUOM,
                                               (_quantity + getPickedQty())).ToString();
            _pickTx.UOM = _salesOrderTable.SDUOM.Trim();
            _pickTx.RFUserID = UserSession.CurrentUser.LoginName;
            _pickTx.Mode = string.Empty;
            _pickTx.TransDate_YYYYMMDD = System.DateTime.Now.ToString("yyyyMMdd");
            _pickTx.P4205Version = WFOptions.RetrieveOptionValue("P4205Version");
            _pickTx.ToLocation = _collectTransferLocation.ScreenInput;
            _pickTx.P4113Version = _transferP4113Version.Trim();
            _pickTx.SecondaryQuantity = string.Empty;
            _pickTx.SecondaryUOM = string.Empty;
            _pickTx.UserInput01 = string.Empty;
            _pickTx.UserInput02 = string.Empty;
            _pickTx.UserInput03 = string.Empty;
            _pickTx.UserInput04 = string.Empty;
            _pickTx.UserInput05 = string.Empty;
            _pickTx.UserInput06 = string.Empty;
            _pickTx.UserInput07 = string.Empty;
            _pickTx.ContainerID = _collectContainer.ScreenInput.Trim();
            if (_isFinished || !_cartonEnabled)
            {
                _pickTx.ManuallySplitSalesOrder = " ";
            }
            else
            {
                _pickTx.ManuallySplitSalesOrder = "1";  
            }
            if (getPickedQty() == 0 || (!_cartonEnabled && !_forceLine))
            {
                _pickTx.ToCartonID = string.Empty;
                _pickTx.CartonQuantity = string.Empty;
                _pickTx.CustomerCartonId = string.Empty;
                _pickTx.CartonCode = string.Empty;
                _pickTx.CartonP4620Version = string.Empty;
            }
            else
            {
                _pickTx.Mode = "1";
                if (_forceLine && !_cartonEnabled)
                {
                    DataTable cartonTemp = _cartonRecords.DefaultView.ToTable(true, new string[] { "location", "container", "lot" });
                    foreach (DataRow dr in cartonTemp.Rows)
                    {
                        setCartonFromLocation(dr["location"].ToString());
                        _pickTx.ContainerID = dr["container"].ToString();
                        _pickTx.Lot = dr["lot"].ToString();
                        _pickTx.Quantity = ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput,
                                                    _salesOrderTable.SDUOM,
                                                    getItemLotContainerQty(dr["location"].ToString(), dr["lot"].ToString(), dr["container"].ToString())).ToString();

                        _pickTx.AddToBoundaryWithData();
                        _pickTx.Mode = "C";
                    }
                    cartonTemp.Dispose();
                }

                if (_cartonEnabled)
                {
                    _pickTx.SerialNumberType = _serialNumberType;
                    foreach (DataRow dr in _cartonRecords.Rows)
                    {
                        _pickTx.Pallet = dr["palletId"].ToString();
                        _pickTx.PalletType = dr["palletType"].ToString();
                 
                        if (_jdeCartonTable.GetCarton(_salesOrderTable.SDMCU.Trim(), dr["carton"].ToString()))
                        {
                            _pickTx.ToCartonID = _jdeCartonTable.CDCRID.ToString();
                            _pickTx.CartonCode = _jdeCartonTable.CDEQTY.Trim();
                        }
                        else
                        {
                            _pickTx.ToCartonID = string.Empty;
                            _pickTx.CartonCode = dr["cartonType"].ToString().Trim();
                        }
                        setCartonFromLocation(dr["location"].ToString());
                        _pickTx.ContainerID = dr["container"].ToString();
                        _pickTx.Lot = dr["lot"].ToString();
                        _pickTx.Quantity = ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput,
                                                    _salesOrderTable.SDUOM,
                                                    getItemLotContainerQty(dr["location"].ToString(), dr["lot"].ToString(), dr["container"].ToString())).ToString();

                        _pickTx.CartonQuantity = ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput,
                                                   _salesOrderTable.SDUOM, double.Parse(dr["quantity"].ToString())).ToString();
                        _pickTx.CustomerCartonId = dr["carton"].ToString().Trim();
                        _pickTx.CartonP4620Version = WFOptions.RetrieveOptionValue("CartonP4620Version");

                        _pickTx.AddToBoundaryWithData();
                        _pickTx.Mode = "C";
                    }
                }
                #region End Doc
                _pickTx.Mode = "2";
                #endregion
                #region Clear Cache
                _pickTx.AddToBoundaryWithData();
                _pickTx.Mode = "3";
                #endregion
            }

            Transactions.Send(WFOptions.RetrieveOptionValue("SyncTransaction"));
            if (!Transactions.Success)
            {
                SendE1Error();
            }
            DisplayMessages();
            deletePickReservation();
            _forceLine = false;
        }
        #endregion
        #region Navigation
        private void decisionTable1()
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    Next(_collectOrder);
                    break;
                case "2": // by PickSlip
                    Next(_collectPickSlip);
                    break;
                case "3": // by Shipment
                    Next(_collectShipment);
                    break;
            } // end switch
        }

        private void decisionTable2(bool printShippingLabel = false)
        {
            clearTransaction();
            bool isDataLeft = callF4211BasicQuery();
            // if a line or item was collected And there are more lines open loop back to collect line
            if ((_collectLine.ScreenInput.Trim().Length > 0 || _collectItem.ScreenInput.Trim().Length > 0) && isDataLeft)
            {
                _collectLine.ScreenInput = string.Empty;
                _collectItem.ScreenInput = string.Empty;
                RestartLoop(_collectLine);
            }
            else if (isDataLeft)
            {
                getPickingData();
                beginPicking();
            }
            else if (printShippingLabel)
            {
                Next(_printShippingLabel);
            }
            else
            {
                _skippedRecords.Clear();
                decisionTable3();
            }
        }

        private void decisionTable3()
        {
            switch (_collectPickMode.ScreenInput.Trim())
            {
                case "1": // by Order
                    RestartLoop(_collectOrder);
                    break;
                case "2": // by PickSlip
                    RestartLoop(_collectPickSlip);
                    break;
                case "3": // by Shipment
                    RestartLoop(_collectShipment);
                    break;
            } // end switch
        }

        private void decisionTable4()
        {
            _locationTable.ResetAll();
            _lotTable.ResetAll();
            ScreenMap.Add("LotNumber", "");
            getPickingDataForceLine(_salesOrderTable.SDLNID.ToString().Trim());
            beginPicking();
        }

        private void clearTransaction()
        {
            _isFinished = false;
            _cartonRecords.Clear();
            _locationTable.ResetAll();
            _lotTable.ResetAll();
            ScreenMap.Add("LotNumber", "");
        }
        #endregion
        #region SendMail
        private void SendZeroMail()
        {
            if (_emailZeroPick.Trim().Length > 0)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(RetrieveVoc("WFUSER") + " " + UserSession.CurrentUser.LoginName + " ");
                sb.Append(RetrieveVoc("PICKDOC") + " " + " " + _salesOrderTable.SDDOCO + " " + _salesOrderTable.SDDCTO + ".\n");
                sb.Append(RetrieveVoc("OriginalQty") + " " + " " + _expectedQuantityInPrimaryUOM.ToString() + "  " + RetrieveVoc("ForItem") + " " + _primaryItem.Trim() + '\n');
                sb.Append(_itemMasterTable.IMDSC1.Trim() + ". " + " " + RetrieveVoc("PickQty") + " " + " ");
                sb.Append("0.\n");
                sb.Append(RetrieveVoc("Occurred") + " " + System.DateTime.Now.Year.ToString() + "-" + System.DateTime.Now.Month.ToString() + "-" + System.DateTime.Now.Day.ToString() + '\n');
                this.SendVarianceNotification("ZeroPick", sb.ToString(), _emailZeroPick);
            }
        }

        private void SendE1Error()
        {
            if (_emailE1Error.Trim().Length > 0)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(RetrieveVoc("WFUSER") + " " + UserSession.CurrentUser.LoginName + " ");
                sb.Append(RetrieveVoc("PICKDOC") + " " + " " + _salesOrderTable.SDDOCO + " " + _salesOrderTable.SDDCTO + ".\n");
                sb.Append(RetrieveVoc("OriginalQty") + " " + " " + _expectedQuantityInPrimaryUOM.ToString() + "  " + RetrieveVoc("ForItem") + " " + _primaryItem.Trim() + '\n');
                sb.Append(_itemMasterTable.IMDSC1.Trim() + ". " + " " + RetrieveVoc("PickQty") + " " + " ");
                sb.Append(ConvertToUOMQty(_collectUnitOfMeasure.ScreenInput,
                                               _salesOrderTable.SDUOM, (_quantity + getPickedQty())).ToString());
                sb.Append(" " + _salesOrderTable.SDUOM + ".\n");
                sb.Append(RetrieveVoc("Transaction") + " " + Transactions.TxId + RetrieveVoc("Errored") + ".\n\n");
                foreach (DataRow dr in Transactions.Messages)
                {
                    sb.Append(UserSession.ScreenHandler.GetErrorMessage(dr["value"].ToString()) + ".\n");
                }
                sb.Append("\n" + RetrieveVoc("Occurred") + " " + System.DateTime.Now.Year.ToString() + "-" + System.DateTime.Now.Month.ToString() + "-" + System.DateTime.Now.Day.ToString() + '\n');
                this.SendVarianceNotification("ZeroPick", sb.ToString(), _emailZeroPick);
            }
        }

        //
        // load column headers in the user's native language
        //
        private string RetrieveVoc(string DataDictionaryItem)
        {
            if (this._vocabTable.RetrieveVocabularyOverride(DataDictionaryItem,
                "S",
                UserSession.ScreenHandler.Environment.ToString(),
                UserSession.ScreenHandler.Language.ToString(),
                "",
                ""))
            {
                return (this._vocabTable.DisplayText);
            }
            else
            {
                return (DataDictionaryItem);
            }
        }
        #endregion
        #region ScreenEvents
        #region BranchPlant
        private void _collectBranch_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateBranch(e.UserInput.ToString().Trim());
        }

        private void validateBranch(string branch)
        {
            if (_pickManagerMode != "0" && !UserSession.IsModuleLicensed("JDPM"))
            {
                throw new RFS_Exception("PickManagerNotLicensed");
            }
            if (int.Parse(_dynamicPick) >= 3 && !UserSession.IsModuleLicensed("JSDP"))
            {
                throw new RFS_Exception("DynamicPickNotLicensed");
            }
            //Query F0006 and F41001 to validate that the branch has open SO Lines. 
            if (!_branchTable.ValidateBranchWithSalesOrders(branch, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode))
            {
                if (!_branchTable.FetchBranchPlant(branch))
                {
                    throw new RFS_Exception("INVALIDBRANCH");
                }
                else
                {
                    throw new RFS_Exception("NoOpenOrders");
                }
            }
            _collectBranch.ScreenInput = _branchTable.MCMCU;
            _warehouseConstantsTable.DeterminePrimaryItem(_collectBranch.ScreenInput);
            Next(_collectPickMode);
        }

        private void _branch_List_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _branchTable.DisplayBranchesWithSalesOrders(_allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
        }

        private void _branch_List_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _branchTable.UpdateCurrentRow();
            validateBranch(_branchTable.MCMCU);
        }
        #endregion
        #region PickMode
        private void _collectPickMode_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validatePickMode(e.UserInput.ToString());
        }

        private void validatePickMode(string pickMode)
        {
            _collectPickMode.ScreenInput = pickMode;
            if (_collectPickMode.ScreenInput.Length == 0)
            {
                throw new RFS_Exception("INVALIDPICKMODE");
            } 
            else
            {
                try
                {
                    int i = Convert.ToInt16(_collectPickMode.ScreenInput);
                    if ((i < 1) || (i > 2))
                    {
                        throw new RFS_Exception("INVALIDPICKMODE");
                    } // end if
                    else
                    {
                        decisionTable1();
                    } // end else
                } // end try
                catch
                {
                    throw new RFS_Exception("INVALIDPICKMODE");
                } // end catch
            } // end else
        }
        #endregion
        #region Order
        private void _collectOrder_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateOrder(e.UserInput.ToString().Trim());
        }

        private void validateOrder(string order)
        {
            
            try
            {
                double dOrder = double.Parse(order);
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            if (!_salesOrderTable.ValidOrder(_collectBranch.ScreenInput, order, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode))
            {
                if (!_salesOrderTable.ValidOrder(_collectBranch.ScreenInput, order, null, null, UserSession.UserId, "0"))
                {
                    throw new RFS_Exception("INVALIDORDER");
                }
                else
                {
                    if (!isValidDocType(_salesOrderTable.SDDCTO))
                    {
                        throw new RFS_Exception("INVALIDORDERTYPE");
                    }
                    else
                    {
                        throw new RFS_Exception("NoOpenLines");
                    }
                }
            }
            _collectOrder.ScreenInput = _salesOrderTable.SDDOCO.ToString();

            // if the document number is unique across all document types, then bypass
            // the document type screen
            if (WFOptions.GetYesNoWFOptionValue("UniqueDocNumber") || isDocTypeUnique())
            {
                if (!removeF4211LocksAndSort())
                {
                    throw new RFS_Exception("AllRecordsLocked");
                }
                _collectOrderType.ScreenInput = _salesOrderTable.SDDCTO;

                Next(_collectPrinter);
            }
            else
            {
                if (!removeF4211LocksAndSort())
                {
                    throw new RFS_Exception("AllRecordsLocked");
                }
                Next(_collectOrderType);
            }
        }

        private bool isDocTypeUnique()
        {
            bool retVal = true;
            string docType = _salesOrderTable.SDDCTO;
            foreach (DataRow dr in _salesOrderTable.Rows)
            {
                _salesOrderTable.CurrentRow = dr;
                if (docType != _salesOrderTable.SDDCTO)
                {
                    retVal = false;
                    break;
                }
            }
            _salesOrderTable.CurrentRow = _salesOrderTable.Rows[0];
            return retVal;
        }

        private bool isValidDocType(string docType)
        {
            
            if (_allowedDocTypes == null || _allowedDocTypes.Length != 0 || _allowedDocTypes[0] != "" || _allowedDocTypes[0] != "*")
            {
                return true;
            }
            
            // determine whether document type of order is allowed
            // based on the workflow options.

            bool found = false;

            for (int i = 0; i < _allowedDocTypes.Length; i++)
            {
                if (_allowedDocTypes[i].ToString().Trim().Length > 0)
                {
                    if (docType.ToUpper().Trim() == _allowedDocTypes[i].ToString().ToUpper().Trim())
                    {
                        found = true;
                    }
                }
                else
                {
                    found = true;
                }
            }

            return (found);
        }

        private void _orderList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.FetchOrders(_collectBranch.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
        }

        private void _orderList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.UpdateCurrentRow();
            _collectOrder.ScreenInput = _salesOrderTable.SDDOCO.ToString();
            validateOrderType(_salesOrderTable.SDDCTO);
        }

        private void _orderList_BackValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            RestartLoop(_collectPickMode);
        }
        #endregion
        #region OrderType

        private void _collectOrderType_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateOrderType(e.UserInput.ToString().Trim());
        }

        private void validateOrderType(string orderType)
        {
            if (!_salesOrderTable.ValidOrder(_collectBranch.ScreenInput, _collectOrder.ScreenInput, orderType, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode))
            {
                throw new RFS_Exception("INVALIDORDERTYPE");
            }
            if (!removeF4211LocksAndSort())
            {
                throw new RFS_Exception("AllRecordsLocked");
            }
            _collectOrderType.ScreenInput = _salesOrderTable.SDDCTO;
            Next(_collectPrinter);
        }

        #endregion
        #region PickSlip
        private void _collectPickSlip_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validatePickSlip(e.UserInput.ToString().Trim());
        }

        private void validatePickSlip(string pickSlip)
        {
            try
            {
                double dPickSlip = double.Parse(pickSlip);
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            if (!_salesOrderTable.ValidPickSlip(_collectBranch.ScreenInput, pickSlip, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode))
            {
                if (!_salesOrderTable.ValidPickSlip(_collectBranch.ScreenInput, pickSlip, null, null, UserSession.UserId, "0"))
                {
                    throw new RFS_Exception("INVALIDPickSlip");
                }
                else
                {
                    if (!isValidDocType(_salesOrderTable.SDDCTO))
                    {
                        throw new RFS_Exception("INVALIDORDERTYPE");
                    }
                    else
                    {
                        throw new RFS_Exception("NoOpenLines");
                    }
                }
            }
            if (!removeF4211LocksAndSort())
            {
                throw new RFS_Exception("AllRecordsLocked");
            }
            _collectPickSlip.ScreenInput = _salesOrderTable.SDPSN.ToString();
            Next(_collectPrinter);
        }

        private void _pickSlipList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.FetchPickSlips(_collectBranch.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
        }

        private void _pickSlipList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.UpdateCurrentRow();
            validatePickSlip(_salesOrderTable.SDPSN.ToString());
        }
        #endregion
        #region Shipment

        private void _collectShipment_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateShipment(e.UserInput.ToString().Trim());
        }

        private void validateShipment(string shipment)
        {
            try
            {
                double dShipment = double.Parse(shipment);
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            if (!_salesOrderTable.ValidShipment(_collectBranch.ScreenInput, shipment, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode))
            {
                if (!_salesOrderTable.ValidShipment(_collectBranch.ScreenInput, shipment, null, null, UserSession.UserId, "0"))
                {
                    throw new RFS_Exception("INVALIDShipment");
                }
                else
                {
                    if (!isValidDocType(_salesOrderTable.SDDCTO))
                    {
                        throw new RFS_Exception("INVALIDORDERTYPE");
                    }
                    else
                    {
                        throw new RFS_Exception("NoOpenLines");
                    }
                }
            }
            if (!removeF4211LocksAndSort())
            {
                throw new RFS_Exception("AllRecordsLocked");
            }
            _collectShipment.ScreenInput = _salesOrderTable.SDSHPN.ToString();
            Next(_collectPrinter);
        }

        private void _shipmentList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.FetchShipments(_collectBranch.ScreenInput, _allowedStatus, _allowedDocTypes, UserSession.UserId, _pickManagerMode);
        }

        private void _shipmentList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {

            _salesOrderTable.UpdateCurrentRow();
            validateShipment(_salesOrderTable.SDSHPN.ToString());
        }
        #endregion
        #region Printer
        private void _collectPrinter_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validatePrinter(e.UserInput.ToString().Trim());
        }

        private void validatePrinter(string printer)
        {
            if (printer.Trim().Length == 0)
            {
                _collectPrinter.ScreenInput = "";
            }
            else
            {
                // Validating that the printer exists for the environment in which they are logged.
                if (this.PrintSystem.ValidPrinter(printer))
                {
                    _collectPrinter.ScreenInput = printer;
                    _printerAddress = PrintSystem.PrinterAddress;
                    ScreenMap.Add("Printer", printer);
                } // end if
                else
                {
                    throw new RFS_Exception(this.PrintSystem.ErrorMessageVocabulary);
                } // end else
            }
        }

        private void _printerList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _printersTable.FetchPrintersByUserWorkflow(UserSession.UserId, WorkflowID);
        }

        private void _printerList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _printersTable.UpdateCurrentRow();
            validatePrinter(_printersTable.PrinterName);
        }

        private void _collectPrinter_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (PrintSystem.DefaultPrinter.Trim().Length == 0)
            {
                Error("LabelFormat");
            }
            else
            {
                if (this.PrintSystem.ValidPrinter(PrintSystem.DefaultPrinter))
                {
                    ScreenHandler.DefaultText = PrintSystem.DefaultPrinter;
                }
            }
        }
        #endregion
        #region Shipping Printer
        private void _collectShippingPrinter_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (!_cartonEnabled)
            {
                _collectShippingPrinter.CancelLoad = true;
                _collectShippingPrinter.ScreenInput = "";
                return;
            }
            if (PrintSystem.DefaultPrinter.Trim().Length == 0)
            {
                Error("LabelFormat");
            }
            else
            {
                if (this.PrintSystem.ValidPrinter(PrintSystem.DefaultPrinter))
                {
                    ScreenHandler.DefaultText = PrintSystem.DefaultPrinter;
                }
            }
        }

        private void _shippingPrinterList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _shippingPrinterList.Table = _printersTable;
            _printersTable.FetchPrintersByUserWorkflow(UserSession.UserId, WorkflowID);
        }

        private bool _collectShippingPrinter_ValidateMethod(string printer)
        {
            if (printer.Trim().Length == 0)
            {
                _collectShippingPrinter.ScreenInput = "";
            }
            else
            {
                // Validating that the printer exists for the environment in which they are logged.
                if (this.PrintSystem.ValidPrinter(printer))
                {
                    _collectShippingPrinter.Validate(printer);
                    _shippingPrinterAddress = PrintSystem.PrinterAddress;
                    ScreenMap.Add("ShippingPrinter", printer);
                } // end if
                else
                {
                    throw new RFS_Exception(this.PrintSystem.ErrorMessageVocabulary);
                } // end else
            }
            return true;
        }
        #endregion 
        #region TransferLocation
        private void _collectTransferLocation_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateTransferLocation(e.UserInput.ToString().Trim());
        }

        private void validateTransferLocation(string location)
        {
            if (!_locationTable.ValidateTransferLocation(_collectBranch.ScreenInput, location, int.Parse(_dynamicPick) != 1))
            {
                if (!_locationTable.ValidateLocation(_collectBranch.ScreenInput, location))
                {
                    throw new RFS_Exception("Location");
                }
                throw new RFS_Exception("SOStageLocation");
            }
            ScreenMap.Add("TransferLocation", _locationTable.sscnlocn);
            _collectTransferLocation.ScreenInput = _locationTable.LMLOCN;
            Next(_collectLine);
        }

        private void _transferLocationList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _locationTable.GetSOTransferLocationsForBranch(_collectBranch.ScreenInput, int.Parse(_dynamicPick) != 1);
        }

        private void _transferLocationList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _locationTable.UpdateCurrentRow();
            validateTransferLocation(_locationTable.LMLOCN);
        }

        private void _collectTransferLocation_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_transferP4113Version.Length == 0)
            {
                _collectTransferLocation.ScreenInput = string.Empty;
                sender.CancelLoad = true;
                sender.NextOverride = _collectLine;
            }
        }
        #endregion
        #region Line
        private void _collectLine_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateLine(e.UserInput.ToString().Trim());
        }

        private void validateLine(string line)
        {
            if (line.Trim().Length == 0)
            {
                _collectLine.ScreenInput = string.Empty;
                return;
            }
            try
            {
                double dline = double.Parse(line);
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            _collectLine.ScreenInput = line;
            getPickingData();
            beginPicking();
        }

        private void _orderDetailList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            callF4211BasicQuery();
            if (!removeF4211LocksAndSort())
            {
                throw new RFS_Exception("AllRecordsLocked");
            }
        }

        private void _orderDetailList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _salesOrderTable.UpdateCurrentRow();
            validateLine(_salesOrderTable.SDLNID.ToString());
        }

        private bool _collectLine_F9Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            _shipLabelReturnScreen = "OrderLine";
            Next(_collectCartonIdPrint);
            return true;
        }
        #endregion
        #region CollectItem
        private void _collectItem_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateItem(e.UserInput.ToString().Trim());

        }

        private void validateItem(string item)
        {
            if (item.Trim().Length == 0)
            {
                _collectItem.ScreenInput = string.Empty;
                _shortItem = double.MinValue;
            }
            else
            {
                _itemMasterTable.ResetAll();

                // Use the returned result to display the proper item number
                _primaryItem = _itemMasterTable.RetrieveItem(_collectBranch.ScreenInput,
                                                            item,
                                                            _warehouseConstantsTable._DesignatedPrimaryItemNumber.ToString());
                if (_primaryItem == null)
                {
                    throw new RFS_Exception("ItemNumber");
                }
                _collectItem.ScreenInput = _primaryItem;
                _shortItem = _itemMasterTable.IMITM;
            }

            getPickingData();
            beginPicking();
        }

        private bool _collectItem_F9Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            _shipLabelReturnScreen = "Item";
            Next(_collectCartonIdPrint);
            return true;
        }
        #endregion
        #region From Location
        private void _collectFromLocation_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateFromLocation(e.UserInput.ToString().Trim());
        }

        private void validateFromLocation(string location)
        {
            //Validate the Location against F4100
            //Validate Item Location againt Suggested We do allow location overrides, but the location must have the item
            if (!_locationTable.ValidateSalesOrderLocation(_collectBranch.ScreenInput, location, !_isHardCommitted && int.Parse(_dynamicPick) != 1))
            {
                if (!_locationTable.ValidateLocation(_collectBranch.ScreenInput, location))
                {
                    throw new RFS_Exception("Location");
                }
                throw new RFS_Exception("SOSupplyLocation");
            }
            if (_isHardCommitted && !_allowOverrideHardCommits && (_expectedHCLocn != _locationTable.stbllocn))
            {
                throw new RFS_Exception("LocationMismatch");
            }
            // item must exist in location.
            if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", 
                // Allow Hard commits to be picked from hard commit location even if it is not a picking location. 
                (!_isHardCommitted || (_expectedHCLocn != _locationTable.stbllocn))  && int.Parse(_dynamicPick) != 1,
                _locationTable.stbllocn, _allowedHoldCodes))
            {
                if (!_itemLocationTable.RetrieveSpecificRecordWithQuantity(_collectBranch.ScreenInput, _shortItem, _locationTable.stbllocn))
                {
                    throw new RFS_Exception("ItemLocation");
                }
                throw new RFS_Exception("ItemHeld");
            }
            ScreenMap.Add("Location", _locationTable.sscnlocn);
            _collectFromLocation.ScreenInput = _locationTable.stbllocn;
            if (_expectedLocn != _locationTable.stbllocn)
            {
                reservePick(_locationTable.stbllocn, _itemLocationTable.LILOTN);
            }
            if (_previousOrder != _salesOrderTable.SDDOCO)
            {
                _collectPallet.ScreenInput = null;
                _previousOrder = _salesOrderTable.SDDOCO;
            }
        }

        private bool _collectFromLocation_F2Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_forceLine)
            {
                throw new RFS_Exception("MustFinishLine");
            }
            Next(_confirmSkipPick);
            return true;
        }

        private bool _collectFromLocation_F6Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_forceLine)
            {
                throw new RFS_Exception("MustFinishLine");
            }
            Next(_confirmZeroPick);
            return true;
        }

        private bool _collectFromLocation_F8Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_forceLine)
            {
                throw new RFS_Exception("MustFinishLine");
            }
            unlockAllRecords();
            _orderDetailList.Show();
            return true;
        }

        private void _fromLocation_List_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", int.Parse(_dynamicPick) != 1, _allowedHoldCodes);
            removeNonHardCommitedLotLocn();
            removeF41021Reservations();
            removeCachedPicks();
        }

        private void _fromLocation_List_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.UpdateCurrentRow();
            validateFromLocation(_itemLocationTable.LILOCN);
        }

        private void _collectFromLocation_BackValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            unlockAllRecords();
            Back(_collectItem);
        }

        private void _collectFromLocation_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            // If the product is hard commited and we don't allow overrides. Verify that it exists in HC Location.
            if (_isHardCommitted && !_allowOverrideHardCommits)
            {
                if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", false, _expectedHCLocn, _allowedHoldCodes, _expectedHCLot))
                {
                    // If Hard Commited and NO Override And No Available Error 
                    Error("HardCommitHold");
                    return;
                }
            }
            else if (_isHardCommitted)
            {
                // If the product is hard commited. Verify that it exists in HC Location or a valid picking location
                if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", false, _expectedHCLocn, _allowedHoldCodes, _expectedHCLot))
                {
                    if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", int.Parse(_dynamicPick) != 1, _allowedHoldCodes))
                    {
                        // If NO Available in branch Error.
                        Error("ItemNotPickable");
                    }
                    return;
                }
            }
            else if (!_itemLocationTable.RetrieveItemLocationRecords(
                    _collectBranch.ScreenInput,
                    _shortItem,
                    "N",
                    int.Parse(_dynamicPick) != 1,
                    _allowedHoldCodes))
            {
                // Check all picking loactions for branch since it is not hard committed.
                Error("ItemNotPickable");
            }

        }

        private bool _collectFromLocation_F9Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            _shipLabelReturnScreen = "FromLocation";
            Next(_collectCartonIdPrint);
            return true;
        }

        #endregion
        #region VerifyItem
        private void _verifyItem_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_defaultVerifyItem)
            {
                ScreenHandler.DefaultText = _itemMasterTable.FetchPrimaryItemNumber(_expectedShortItem,
                                                _warehouseConstantsTable._DesignatedPrimaryItemNumber);
            }
        }

        private void _verifyItem_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            verifyItem(e.UserInput.ToString().Trim());
        }

        private void verifyItem(string item)
        {
            _itemMasterTable.ResetAll();

            // Use the returned result to display the proper item number
            string tempPrimaryItem = _itemMasterTable.RetrieveItem(_collectBranch.ScreenInput,
                                                        item,
                                                        _warehouseConstantsTable._DesignatedPrimaryItemNumber.ToString());

            if (tempPrimaryItem == null)
            {
                throw new RFS_Exception("ItemNumber");
            }
            if (_expectedShortItem != _itemMasterTable.IMITM)
            {
                throw new RFS_Exception("ItemMismatch");
            }
        }
        #endregion
        #region Lot
        private void _collectLot_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            ScreenMap.Add("LotNumber", "");
            if (!isLotItem())
            {
                _collectLot.CancelLoad = true;
                _collectLot.ScreenInput = string.Empty;
                Next(_collectContainer);
                return;
            }
        }

        private void _collectLot_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateLot(e.UserInput.ToString().Trim());
        }

        private void validateLot(string lot)
        {
            if (_isHardCommitted && _expectedHCLot.Trim().ToUpper() != lot.Trim().ToUpper())
            {
                throw new RFS_Exception("LotMismatch");
            }
            if (lot == string.Empty && Convert.ToInt16(_itemBranchTable.IBSRCE) == Convert.ToInt16(LotSerialDesignations.LotOptional))
            {
                _collectLot.ScreenInput = "";
                ScreenMap.Add("LotNumber", "");
                return;
            }
            if (!_lotTable.RetrieveLotMaster(_collectBranch.ScreenInput, _shortItem, lot))
            {
                throw new RFS_Exception("LotNumber");
            }
            if (DateTimeUtils.JDEJulian(_lotTable.IOMMEJ) < System.DateTime.Today)
            {
                throw new RFS_Exception("ExpiredLot");
            }
            if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", 
                (!_isHardCommitted) ? int.Parse(_dynamicPick) != 1 : false, _collectFromLocation.ScreenInput, _allowedHoldCodes, lot))
            {
                if (!_itemLocationTable.RetrieveSpecificRecordWithQuantity(_collectBranch.ScreenInput, _shortItem, _locationTable.LMLOCN, lot))
                {
                    throw new RFS_Exception("ItemLocation");
                }
                throw new RFS_Exception("ItemHeld");
            }
            if (_expectedLocn.Trim().ToUpper() != _locationTable.stbllocn.Trim().ToUpper() || _expectedLot.Trim().ToUpper() != _lotTable.IOLOTN.Trim().ToUpper())
            {
                reservePick(_locationTable.stbllocn, _lotTable.IOLOTN);
            }
            _collectLot.ScreenInput = _lotTable.IOLOTN;
            ScreenMap.Add("LotNumber", _lotTable.IOLOTN);
        }

        private void _lotList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", int.Parse(_dynamicPick) != 1, _collectFromLocation.ScreenInput, _allowedHoldCodes);
            removeNonHardCommitedLotLocn();
            removeF41021Reservations();
        }

        private void _lotList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.UpdateCurrentRow();
            validateLot(_itemLocationTable.LILOTN);
        }
        #endregion
        #region Container
        private void _collectContainer_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _collectContainer.ScreenInput = e.UserInput.ToString().ToUpper();
            if (_collectSerial.ScreenInput != null && _collectSerial.ScreenInput.Trim().Length > 0)
            {
                // the user collected a Serial Number
                if (_cartonEnabled)
                {
                    Next(_collectPallet);
                }
                else
                {
                    checkPartialAndSend();
                    Next(_printBarcodeLabels);
                }
            }
            if (_forceLine)
            {
                _expectedQuantityInPrimaryUOM += _conversionFactor * getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput);
                // The UOM must not be changed.
                Next(_collectQuantity);
            }
        }

        private void _collectContainer_BackValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_collectSerial.ScreenInput != null && _collectSerial.ScreenInput.Trim().Length > 0)
            {
                Back(_collectSerial);
            }
            else
            {
                Back(_collectFromLocation);
            }
        }

        private void _collectContainer_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_defaultCNID && _collectContainer.ScreenInput != null && _collectContainer.ScreenInput.Trim().Length > 0)
            {
                ScreenHandler.DefaultText = _collectContainer.ScreenInput;
            }
        }
        #endregion
        #region UOM
        private void _collectUnitOfMeasure_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            ScreenHandler.DefaultText = _defaultUOM;
        }

        private void _collectUnitOfMeasure_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateUOM(e.UserInput.ToString().Trim());
        }

        private void validateUOM(string uom)
        {
            _conversionFactor = ConvertToUOMQty(uom, _itemMasterTable.IMUOM1.ToString(), 1);
            if (_conversionFactor == 0)
            {
                throw new RFS_Exception("UnitOfMeasure");
            }
            if (!_allowLargerUOM && _conversionFactor > _expectedQuantityInPrimaryUOM)
            {
                throw new RFS_Exception("UOMLargerThanPick");
            }
            _collectUnitOfMeasure.ScreenInput = uom;

        }

        private void _unitOfMeasure_List_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _uomConversionTable.FetchUOMConversions(_collectBranch.ScreenInput, _shortItem);
        }

        private void _unitOfMeasure_List_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _uomConversionTable.UpdateCurrentRow();
            validateUOM(_uomConversionTable.UMUM);
        }
        #endregion
        #region Quantity
        private void _collectQuantity_BackValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _cartonRecords.Clear();
            Back(_collectUnitOfMeasure);
        }

        private void _collectQuantity_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateQuantity(e.UserInput.ToString());
        }

        private void validateQuantity(string quantity)
        {
            // Call calculator function
            try
            {
                string calculatedQuantity = _itemLocationTable.CalcQty(quantity);

                if (calculatedQuantity == null)
                {
                    throw new RFS_Exception("InvalidQuantity");
                } // end if
                else if (double.Parse(calculatedQuantity) >= 0)
                {
                    _quantity = double.Parse(calculatedQuantity);
                } // end if
                else
                {
                    throw new RFS_Exception("NegativeEntry");
                } // end else
            } // end try
            catch
            {
                throw new RFS_Exception("InvalidQuantity");
            } // end catch
            if (isOverPick())
            {
                throw new RFS_Exception("OverPickNotAllowed");
            }
            if (!isProductAvailable())
            {
                throw new RFS_Exception("AvailableQty");
            }
            ScreenHandler.ScreenMap.Add("CollectedQuantity", _quantity.ToString());

            double totalQuantity = _quantity + getPickedQty();
            ScreenMap.Add("PickQuatity", totalQuantity.ToString());
            if (_cartonEnabled)
            {
                Next(_collectPallet);
            }
            else if (isShortPick())
            {
                // Not Carton
                Next(_shortPickWarning);
            }
            else
            {
                checkPartialAndSend();
                Next(_printBarcodeLabels);
            }
        }

        private void _collectQuantity_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (_conversionFactor <= 0)
                convertRemainSOQuantityToPrimaryUOM(false);
            ScreenMap.Add("Picked", getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput).ToString());
            ScreenMap.Add("RemainingQtyInCollectedUOM", ((_expectedQuantityInPrimaryUOM / _conversionFactor)
                - getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput)).ToString());
            ScreenMap.Add("CollectedUOM", _collectUnitOfMeasure.ScreenInput);
        }

        private bool _collectQuantity_F6Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (!_cartonEnabled || getPickedQty() == 0)
            {
                throw new RFS_Exception("InvalidKey");
            }
            if (_salesOrderTable.SDAPTS == "N")
            {
                Next(_shortPickWarning);
            }
            else
            {
                Next(_cartonShortPickOptions);
            }
            return true;
        }
        #endregion
        #region ShortPick
        private void _shortPickWarning_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateShortPickWarning(e.UserInput.ToBool());
        }

        private void validateShortPickWarning(bool accept)
        {
            if (accept)
            {
                handlePartialShipment();
            }
            else if (!accept)
            {
                Back(_collectQuantity);
            }
            else
            {
                throw new RFS_Exception("InvalidEntry");
            }
        }

        private void checkPartialAndSend()
        {
            if (isE1ShortPick())
            {
                handlePartialShipment();
            }
            else if (_forceLine)
            {
                if(!_cartonEnabled)
                    addCarton("", "");
                SendToERP();
            }
            else
            {
                SendToERP();
            }
        }

        private void handlePartialShipment()
        {
            if (_salesOrderTable.SDAPTS == "N")
            {
                _forceLine = true;
                if (_quantity > 0 && !_cartonEnabled)
                {
                    addCarton("", "");
                }
            }
            else
            {
                SendToERP();
            }
        }

        private void _shortPickWarning_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            double totalQuantity = _quantity + getPickedQty();
            ScreenMap.Add("OrderQtyInCollectedUOM", ConvertToUOMQty(_salesOrderTable.SDUOM, _collectUnitOfMeasure.ScreenInput, _salesOrderTable.SDSOQS).ToString());
            ScreenMap.Add("PickQuantity", totalQuantity.ToString());
            ScreenMap.Add("PickUOM", _collectUnitOfMeasure.ScreenInput);
        }
        #endregion
        #region Serial
        private void _collectSerial_BackValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            unlockAllRecords();
            Back();
        }

        private void _collectSerial_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateSerialNumber(e.UserInput.ToString().Trim());
        }

        private void validateSerialNumber(string serialNumber)
        {
            if (serialNumber == string.Empty)
            {
                throw new RFS_Exception("BlanksNotAllowed");
            }
            if (!_lotTable.RetrieveLotMaster(_collectBranch.ScreenInput, _shortItem, serialNumber))
            {
                throw new RFS_Exception("SerialNumber");
            }
            if (DateTimeUtils.JDEJulian(_lotTable.IOMMEJ) < System.DateTime.Today)
            {
                throw new RFS_Exception("ExpiredSerial");
            }
            if (!_itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", int.Parse(_dynamicPick) != 1, null, _allowedHoldCodes, serialNumber))
            {
                if (!_itemLocationTable.RetrieveSpecificRecordWithQuantity(_collectBranch.ScreenInput, _shortItem, null, serialNumber))
                {
                    throw new RFS_Exception("ItemLocation");
                }
                if (!_itemLocationTable.RetrieveItemLocationRecords(
                        _collectBranch.ScreenInput,
                        _shortItem,
                        "N",
                        int.Parse(_dynamicPick) != 1,
                        null,
                        null,
                        serialNumber))
                {
                    throw new RFS_Exception("SOSupplyLocation");
                }
                throw new RFS_Exception("ItemHeld");
            }
            if (!_locationTable.ValidateSalesOrderLocation(_collectBranch.ScreenInput, _itemLocationTable.LILOCN, int.Parse(_dynamicPick) != 1))
            {
                throw new RFS_Exception("Location");
            }
            if (_isHardCommitted && _expectedHCLot.Trim() != serialNumber.Trim())
            {
                if (!validOrderSerial(serialNumber))
                {
                    throw new RFS_Exception("LotMismatch");
                }
            }
            _collectSerial.ScreenInput = _lotTable.IOLOTN;
            ScreenMap.Add("Location", _locationTable.sscnlocn);
            ScreenMap.Add("LotNumber", _lotTable.IOLOTN);
            _collectFromLocation.ScreenInput = _locationTable.stbllocn;
            _collectLot.ScreenInput = _lotTable.IOLOTN;
            _quantity = 1;
            _collectQuantity.ScreenInput = "1";
            _collectUnitOfMeasure.ScreenInput = _itemMasterTable.IMUOM1;
            if (_expectedLocn != _locationTable.stbllocn || _expectedLot.Trim() != _lotTable.IOLOTN)
            {
                reservePick(_locationTable.stbllocn, _lotTable.IOLOTN);
            }
        }

        private bool validOrderSerial(string serial)
        {
            DataRow currentRow = _salesOrderTable.CurrentRow;
            string currentFilter = _salesOrderTable.ThisDataTable.DefaultView.RowFilter;
            if (currentFilter == string.Empty)
            {
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter = " SDITM = " + _shortItem;
            }
            else
            {
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter += " AND SDITM = " + _shortItem;
            }
            _salesOrderTable.ThisDataTable.DefaultView.RowFilter += " AND SDLOTN = '" + serial + "'";
            if (_salesOrderTable.ThisDataTable.DefaultView.Count == 0)
            {
                _salesOrderTable.ThisDataTable.DefaultView.RowFilter = currentFilter;
                _salesOrderTable.CurrentRow = currentRow;
                return false;
            }
            else
            {
                _salesOrderTable.CurrentRow = _salesOrderTable.ThisDataTable.DefaultView[0].Row;
                loadScreenMapsFromF4211();
                return true;
            }
        }

        private bool _collectSerial_F2Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            Next(_confirmSkipPick);
            return true;
        }

        private bool _collectSerial_F8Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            throw new RFS_Exception("InvalidButton");
            return true;
        }

        private bool _collectSerial_F6Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            Next(_confirmZeroPick);
            return true;
        }

        private void _availableSerialsList_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.RetrieveItemLocationRecords(_collectBranch.ScreenInput, _shortItem, "N", int.Parse(_dynamicPick) != 1, _allowedHoldCodes);
            removeF41021Reservations();
            removeNonHardCommitedLotLocn();
        }

        private void _availableSerialsList_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _itemLocationTable.UpdateCurrentRow();
            validateSerialNumber(_itemLocationTable.LILOTN);
        }
        #endregion
        #region Pallet
        private void _collectPallet_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _palletType = string.Empty;
            ScreenMap.Add("Pallet", "");
            if (!_cartonEnabled)
            {
                _collectPallet.CancelLoad = true;
                _collectPallet.ScreenInput = "";
                Next(_collectCartonId);
            }
            if (_collectPallet.ScreenInput != null && _collectPallet.ScreenInput.Trim().Length > 0 && _defaultPalletID)
            {
                ScreenHandler.DefaultText = _collectPallet.ScreenInput.Trim();
            }
        }

        private bool _collectPallet_ValidateMethod(string pallet)
        {
            validatePallet(pallet);
            Next(_collectCartonId);
            return true;
        }

        private void validatePallet(string pallet)
        {
            if (pallet.Length != 0)
            {
                if (!_rfsCartonTable.GetCarton(pallet) & !_jdeCartonTable.GetCarton(_collectBranch.ScreenInput, pallet))
                {
                    throw new RFS_Exception("Pallet");
                }

                if ((_rfsCartonTable.Rows.Count > 0) && (_forceBranch) && (_rfsCartonTable.CLMCU.Trim().ToUpper() != _collectBranch.ScreenInput.Trim().ToUpper()))
                {
                    throw new RFS_Exception("BranchMismatch");
                }
                if (_jdeCartonTable.Rows.Count > 0)
                {
                    _palletType = _jdeCartonTable.CDEQTY;
                    if (_salesOrderTable.SDSHPN != _jdeCartonTable.CDSHPN)
                    {
                        throw new RFS_Exception("CartonShipment");
                    }
                }
                else
                {
                    _palletType = _rfsCartonTable.CLEQTY;
                }
            }
            ScreenMap.Add("PalletType", _palletType);
            ScreenMap.Add("Pallet", pallet);
            _collectPallet.Validate(pallet);
        }
        #endregion
        #region Carton
        private void _collectCartonId_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            double TotalPicked = getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput) + _quantity;
            ScreenMap.Add("Picked", TotalPicked.ToString());
            if (!isSerialItem())
            {
                ScreenMap.Add("RemainingQtyInCollectedUOM", ((_expectedQuantityInPrimaryUOM / _conversionFactor) - TotalPicked).ToString());
            }
            else
            {
                ScreenMap.Add("RemainingQtyInCollectedUOM", ((_expectedQuantityInPrimaryUOM) - TotalPicked).ToString());
            }
            ScreenMap.Add("CollectedUOM", _collectUnitOfMeasure.ScreenInput);
        }

        private void _collectCartonId_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateCartonId(e.UserInput.ToString().Trim());
        }

        private void validateCartonId(string carton)
        {
            if (!_rfsCartonTable.GetCarton(carton))
            {
                throw new RFS_Exception("CartonId");
            }
            if ((_forceBranch) && (_rfsCartonTable.CLMCU.Trim().ToUpper() != _collectBranch.ScreenInput.Trim().ToUpper()))
            {
                throw new RFS_Exception("BranchMismatch");
            }
            if (_jdeCartonTable.GetCarton(_collectBranch.ScreenInput, carton))
            {
                if (_salesOrderTable.SDSHPN != _jdeCartonTable.CDSHPN)
                {
                    throw new RFS_Exception("CartonShipment");
                }
            }
            addCarton(carton, _rfsCartonTable.CLEQTY);
            if ((_collectSerial.ScreenInput != null && _collectSerial.ScreenInput.Trim().Length > 0) || !isShortPick())
            {
                checkPartialAndSend();
                Next(_printBarcodeLabels);
            }
            else
            {
                Next(_collectQuantity);
            }
        }
        #endregion
        #region PrintBarcodeLabels
        private void _printBarcodeLabels_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            if (Transactions.Error && _collectPrinter.ScreenInput.Trim().Length > 0 && !_forceLine)
            {
                _printBarcodeLabels.CancelLoad = true;
                restartPickLoop();
            }
        }

        private void _printBarcodeLabels_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            int numberOfLabels = 0;
            try
            {
                numberOfLabels = int.Parse(e.UserInput.ToString());
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            if (numberOfLabels < 0)
            {
                throw new RFS_Exception("NegativeNotAllowed");
            }
            if (numberOfLabels > _maxLabels)
            {
                throw new RFS_Exception("GreaterThanMax");
            }
            if (numberOfLabels > 0)
            {
                printBarCode(numberOfLabels);
            }
            restartPickLoop();
        }

        private void restartPickLoop()
        {
            unlockAllRecords();
            if (!_forceLine)
            {
                decisionTable2(_cartonEnabled);
            }
            else
            {
                decisionTable4();
            }
        }

        private bool RemoveOtherLinesForPrint()
        {
            for (int i = 0; i < _salesOrderTable.Rows.Count; i++)
            {
                if (_salesOrderTable.CurrentRow != _salesOrderTable.Rows[i])
                {
                    _salesOrderTable.Rows[i].Delete();
                }
                _salesOrderTable.ThisDataTable.AcceptChanges();
            }
            return true;
        }

        private void printBarCode(int numberOfLabels)
        {
            RemoveOtherLinesForPrint();
            PrintSystem.Reset();
            PrintSystem.Branch = _collectBranch.ScreenInput;
            PrintSystem.ItemNumber = _primaryItem;
            PrintSystem.Location = _collectFromLocation.ScreenInput;

            PrintSystem.DropFilePathOverride = WFOptions.RetrieveOptionValue("DropFilePath");
            PrintSystem.PrinterAddress = _printerAddress;
            PrintSystem.PrinterName = _collectPrinter.ScreenInput;
            PrintSystem.Copies = numberOfLabels;

            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_Container", _collectContainer.ScreenInput, false);
            double totalQuantity = _quantity + getItemLotQty(_collectFromLocation.ScreenInput, _collectLot.ScreenInput); 
            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_TransactionQuantity", totalQuantity.ToString(), false);
            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_UnitOfMeasure", _collectUnitOfMeasure.ScreenInput, false);
            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_FromLocation", _collectFromLocation.ScreenInput, false);
            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_TransferLocation", _collectTransferLocation.ScreenInput, false);
            PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_PrimaryItemNumber", _primaryItem, false);
            PrintSystem.ExtraPrintData.NewRow();
            if (!PrintSystem.Print())
            {
                DisplayMessage(PrintSystem.ErrorMessageVocabulary);
            }
        }
        #endregion
        #region ConfirmZeroPick
        private void _confirmZeroPick_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateConfirmZeroPick(e.UserInput.ToBool());

        }

        private void validateConfirmZeroPick(bool confirm)
        {
            if (confirm)
            {
                _collectFromLocation.ScreenInput = _expectedLocn;
                zeroPick();
                unlockAllRecords();
                decisionTable2();
            }
            else if (!confirm)
            {
                Back();
            }
            else
            {
                throw new RFS_Exception("InvalidEntry");
            }
        }
        #endregion
        #region ConfirmSkipPick
        private void _confirmSkipPick_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateConfirmSkipPick(e.UserInput.ToBool());
        }

        private void validateConfirmSkipPick(bool confirm)
        {
            if (confirm)
            {
                skipPick();
            }
            else if (!confirm)
            {
                Back();
            }
            else
            {
                throw new RFS_Exception("InvalidEntry");
            }
        }
        #endregion
        #region Carton Short Pick
        private void _cartonShortPickOptions_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            validateCartonShortPickOptions(e.UserInput.ToString());
        }

        private void validateCartonShortPickOptions(string option)
        {
            int shortOption = 0;
            try
            {
                shortOption = int.Parse(option);
            }
            catch
            {
                throw new Exception("InvalidInput");
            }
            if (shortOption < 1 || shortOption > 3)
            {
                throw new Exception("InvalidInput");
            }

            switch (shortOption)
            {
                case 1: // Collect Quanity
                    Back(_collectQuantity);
                    break;
                case 2: // Leave shipment open
                    _isFinished = false;
                    checkPartialAndSend();
                    break;
                case 3: // Finish Shipment
                    _isFinished = true;
                    checkPartialAndSend();
                    break;
            } // end switch
        }

        private void _cartonShortPickOptions_LoadEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            double totalQuantity = _quantity + getPickedQty();
            if (int.Parse(_dynamicPick) < 3 || _collectLine.ScreenInput.Trim().Length > 0 || _collectItem.ScreenInput.Trim().Length > 0)
            {
                ScreenMap.Add("OrderQtyInCollectedUOM", ConvertToUOMQty(_salesOrderTable.SDUOM, _collectUnitOfMeasure.ScreenInput, _salesOrderTable.SDSOQS).ToString());
            }
            else
            {
                ScreenMap.Add("OrderQtyInCollectedUOM", ConvertToUOMQty(_itemMasterTable.IMUOM1, _collectUnitOfMeasure.ScreenInput, double.Parse(_salesOrderTable["QtyToPick"].ToString())).ToString());
            }
            ScreenMap.Add("PickQuantity", totalQuantity.ToString());
            ScreenMap.Add("PickUOM", _collectUnitOfMeasure.ScreenInput);
        }
        #endregion
        #region Print Shipping Label Y/N
        private void _printShippingLabel_EnterValidationEvent(RFSScreenControl sender, ScreenEventArgs e)
        {
            _shipLabelReturnScreen = string.Empty;
            try
            {
                if (e.UserInput.ToBool())
                {
                    Next(_collectCartonIdPrint);
                }
                else
                {
                    decisionTable2();
                }
            }
            catch
            {
                throw new RFS_Exception("InvalidEntry");
            }
        }
        #endregion
        #region CartonIdPrint
        private bool _collectCartonIdPrint_ValidateMethod(string carton)
        {
            if (!_jdeCartonTable.GetCarton(_collectBranch.ScreenInput, carton))
            {
                throw new RFS_Exception("Carton");
            }
            ScreenMap.Add("ShippingCarton", carton);
            _collectCartonIdPrint.Validate(carton);
            return true;
        }

        private bool _collectCartonIdPrint_F2Method(RFSScreenControl sender, ScreenEventArgs e)
        {
            switch (_shipLabelReturnScreen)
            {
                case "OrderLine":
                    Back(_collectLine);
                    break;
                case "Item":
                    Back(_collectItem);
                    break;
                case "FromLocation":
                    Back(_collectFromLocation);
                    break;
                default:
                    decisionTable2();
                    break;
            }
            return true;
        }
        #endregion

        #region Print shipping Bar Code Labels
        private bool _PrintShippingBarCodeLabels_ValidateMethod(string numberOfLabels)
        {
            int copies = 0;
            try
            {
                copies = int.Parse(numberOfLabels);
            }
            catch
            {
                throw new RFS_Exception("NotNumeric");
            }
            if (copies < 0)
            {
                throw new RFS_Exception("NegativeNotAllowed");
            }
            if (copies > _maxLabels)
            {
                throw new RFS_Exception("GreaterThanMax");
            }
            if (copies > 0)
            {
                printShippingBarCode(copies);
            }
            Back(_collectCartonIdPrint);
            return true;
        }

        private void printShippingBarCode(int copies)
        {
            // Get data for label
            F4211 salesOrderDetailCarton = new F4211(UserSession.DataAccess, "Carton");
            _jdeCartonTable.GetCartonForPrint(_collectBranch.ScreenInput, _jdeCartonTable.CDCRID);
            ParseCarton(salesOrderDetailCarton);
            _jdeCartonTable.GetCarton(_collectBranch.ScreenInput, _collectCartonIdPrint.ScreenInput);
            F0101 toAddrTable = new F0101(UserSession.DataAccess, "To");
            F0101 fromAddrTable = new F0101(UserSession.DataAccess, "From");
            F0101 carrierAddrTable = new F0101(UserSession.DataAccess, "Carrier");
            F0116 toAddrTable2 = new F0116(UserSession.DataAccess, "To");
            F0116 fromAddrTable2 = new F0116(UserSession.DataAccess, "From");
            F0116 carrierAddrTable2 = new F0116(UserSession.DataAccess, "Carrier");
            toAddrTable.RetrieveAddress(salesOrderDetailCarton.SDAN8.ToString());
            fromAddrTable.RetrieveAddress(_branchTable.MCAN8.ToString());
            carrierAddrTable.RetrieveAddress(salesOrderDetailCarton.SDCARS.ToString());
            toAddrTable2.RetrieveAddress(salesOrderDetailCarton.SDAN8.ToString());
            fromAddrTable2.RetrieveAddress(_branchTable.MCAN8.ToString());
            carrierAddrTable2.RetrieveAddress(salesOrderDetailCarton.SDCARS.ToString());
            PrintSystem.Reset();
            PrintSystem.Branch = _collectBranch.ScreenInput;

            PrintSystem.Location = salesOrderDetailCarton.SDLOCN;

            PrintSystem.DropFilePathOverride = WFOptions.RetrieveOptionValue("DropFilePath");
            PrintSystem.PrinterAddress = _shippingPrinterAddress;
            PrintSystem.PrinterName = _collectShippingPrinter.ScreenInput;
            PrintSystem.Copies = copies;
            if (_isMixed)
            {
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_ShortItemNumber", "MIXED", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_LongItemNumber", "MIXED", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_ThirdItemNumber", "MIXED", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_Description1", "Mixed Items", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_Description2", "", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_PrimaryUOM", "", false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_UPCNumber", "", false);
            }
            else
            {
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_ShortItemNumber", _itemMasterTable.IMITM.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_LongItemNumber", _itemMasterTable.IMLITM.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_ThirdItemNumber", _itemMasterTable.IMAITM.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_Description1", _itemMasterTable.IMDSC1.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_Description2", _itemMasterTable.IMDSC2.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_PrimaryUOM", _itemMasterTable.IMUOM1.ToString(), false);
                PrintSystem.ExtraPrintData.AddColumnAndSetDefaultData("_UPCNumber", _itemMasterTable.IMUPCN.ToString(), false);
            }
            PrintSystem.ExtraPrintData.NewRow();
            if (!PrintSystem.Print())
            {
                DisplayMessage(PrintSystem.ErrorMessageVocabulary);
            }
            toAddrTable = null;
            fromAddrTable = null;
            carrierAddrTable = null;
            toAddrTable2 = null;
            fromAddrTable2 = null;
            carrierAddrTable2 = null;
        }

        private void ParseCarton(F4211 salesOrderForCarton)
        {
            _isMixed = false;
            double lastItem = 0;
            string cartons = "";
            foreach (DataRow dr in _jdeCartonTable.Rows)
            {
                _jdeCartonTable.CurrentRow = dr;
                if (!cartons.Contains(_jdeCartonTable.CDCRID.ToString()))
                {
                    cartons += _jdeCartonTable.CDCRID.ToString() + ", ";
                }
                if (_jdeCartonTable.CDITM != 0)
                {
                    if (_jdeCartonTable.CDITM != lastItem && lastItem != 0)
                    {
                        _isMixed = true;
                        return;
                    }
                    lastItem = _jdeCartonTable.CDITM;
                    salesOrderForCarton.IsKeyValid(_jdeCartonTable.CDMCU, _jdeCartonTable.CDDOCO, _jdeCartonTable.CDDCTO,
                        _jdeCartonTable.CDKCOO, _jdeCartonTable.CDLNID, _jdeCartonTable.CDSFXO);
                    _itemMasterTable.FetchItem(salesOrderForCarton.SDITM);
                }
            }
            if (cartons.Length > 0)
            {
                if (_jdeCartonTable.GetCartonsForPrint(_collectBranch.ScreenInput, cartons.Substring(0, cartons.Length - 2)))
                {
                    // The F4620 table is recursive and a carton can be in a carton and have items. 
                    // We are trying to figure out if there are multiple items in the carton.
                    ParseCarton(salesOrderForCarton);
                }
            }
        }
        #endregion
        #endregion
    }
}