with New_Lasts;
use  New_Lasts;

with Market_Codes;
use  Market_Codes;

with Sparta.Choosers;
use  Sparta.Choosers;

with Sparta.Dealers_Manager;
use  Sparta.Dealers_Manager;

with Sparta.Stubs.Types.Ops;
use  Sparta.Stubs.Types.Ops;

with Sparta.Market_Data.Consumers.Stubs;
use  Sparta.Market_Data.Consumers.Stubs;

with Sparta.Market_Data.Phase_Support;
use  Sparta.Market_Data.Phase_Support;

with Sparta.TR.Fid_Fields.Stubs;
use  Sparta.TR.Fid_Fields.Stubs;

with Sparta.Market_Data.Prices;
use  Sparta.Market_Data.Prices;

with Time_Zones;
use  Time_Zones;

package body Sparta.TR.Exchange_Specifics.Thailand_Stocks is
   use Sparta.Market_Data.Lasts;
   use Sparta.TR.Stub_Types.Enum_T;
   use Sparta.Stubs.Types.Duration_T;

   prices_timestamps                : constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class :=
     Use_First(B_QUOTE_DATE+S_QUOTIM_NS,Received);
   prices_timestamps_opening_dealer : constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class :=
     B_TRADE_DATE+Use_First(S_SALTIM_NS, Use_First(S_SALTIM_MS,S_TIMACT));
   prices_timestamps_closing_dealer : constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class :=
     B_OFF_CLS_DT+S_TIMACT;
   prices_timestamps_auction_dealer : constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class :=
     Use_First(B_QUOTE_DATE+Use_First(S_INDAUC_NS,S_TIMACT),Combine(Received,Use_First(S_INDAUC_NS,S_TIMACT)));
   prices_timestamps_last_dealer    : constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class :=
     B_TRADE_DATE+Use_First(S_SALTIM_NS,Use_First(S_SALTIM_MS,Use_First(S_SALTIM,S_TIMACT)));

   compute_EOD_trades         : constant access constant Boolean_T.Target_Stub_Descriptor_Type'Class :=
     S_IRGCOND <= 32764 and Is_Blank(S_TRDVOL_1);
   compute_EOD_auction_states : constant access constant Boolean_T.Target_Stub_Descriptor_Type'Class :=
     (Is_Valid(S_IND_AUC) or Is_Valid(S_IND_AUCVOL) or
        Is_Valid(S_BID) or Is_Valid(S_ASK)
        or Is_Valid(S_BIDSIZE) or Is_Valid(S_ASKSIZE));

begin
   Set_Global_Chooser
     (Data_Vendor (TR.Reuters_Data_Vendor));
   -- !!!!!!!!     Add "Register" after this line.     !!!!!!!!!
   -- Thailand Stock Exchange

   Register
     (Mkt (MKT_XBKK),
      Tag => "Thailand_Stocks",
      Descriptors =>
        (TR_Standard_L1_Phase_Dealer
           (Stamp   => Use_First(Combine(Received,S_TIMACT),Received),
            Compute => Changed (B_INST_PHASE),
            Map     =>
              ((No_Trading_Phase => B_INST_PHASE <= (1,7,9),
                Trading_Phase    => B_INST_PHASE <= 3,
                Auction_Phase    => B_INST_PHASE <= (2,5,6,14),
                --Trade_At_Last_Phase => B_INST_PHASE <= null,         --TR_WSUG.DE_L1.csv.gz trade_at_last example
                Unknown_Phase    => not Is_Valid(B_INST_PHASE),
                others           => null))),

         TR_Standard_L1_Price_Dealer_B_Nondeprecated
           (Stamp => Use_First(prices_timestamps,Received),

            Size  =>
              (Bid => Construct_Unsigned_Volume
                 (Info_Volume_Lot_Multiplier *
                    If_Then_Else
                      (Condition   => B_BID_TONE <= + "M",
                       Then_Clause => B_MKOBID_VOL,
                       Else_Clause => If_Then_Else
                         (Is_Blank(B_BID) and not(Is_Blank(B_BIDSIZE)),
                          Scalar_T.Blank_Stub,
                          B_BIDSIZE))),

               Ask => Construct_Unsigned_Volume
                 (Info_Volume_Lot_Multiplier *
                    If_Then_Else
                      (Condition   => B_ASK_TONE <= + "M",
                       Then_Clause => B_MKOASK_VOL,
                       Else_Clause => If_Then_Else
                         (Is_Blank(B_ASK) and not(Is_Blank(B_ASKSIZE)),
                          Scalar_T.Blank_Stub,
                          B_ASKSIZE)))),


            Compute  => A_Compute_Nondeprecated_B
              (Target => prices_timestamps),

            Assertions =>
              (Assert_Currency_L1,
               Assert_Currency_Blank_Or_Zero_L1,
               Assert_Currency_Multiplier_L1,
               Assert_Deprecated_Quote_Ask (Target => prices_timestamps),
               Assert_Deprecated_Quote_Bid (Target => prices_timestamps))
                & Assert_Price_Not_Valid_When_Not_Compute_Nondeprecated
                  (Compute_Label => "TR_QUOTE_B_COMPUTE",
                   Compute       => Compute_B_Updates (Timestamp => prices_timestamps))),

         TR_Standard_L1_Opening_Price_Dealer
           (Stamp      => Use_First(prices_timestamps_opening_dealer, Received),
            Compute    => A_Clopen_Compute,
            Assertions => (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_Not_Valid_When_Not_Compute ("TR_OPEN_COMPUTE", S_OPEN_PRC))),

         TR_Standard_L1_Closing_Price_Dealer
           (Stamp      => Use_First(prices_timestamps_closing_dealer, Received),
            Compute    => A_Clopen_Compute,
            Assertions => (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_Not_Valid_When_Not_Compute ("TR_CLOSE_COMPUTE", S_OFF_CLOSE))),

         TR_Standard_L1_Auction_State_Dealer_Nondeprecated
           (TimeStamp      => prices_timestamps_auction_dealer,
            Exchange_Stamp => prices_timestamps_auction_dealer,
            Compute        => compute_EOD_auction_states and
              A_Compute_Nondeprecated_S
                (Target  => prices_timestamps_auction_dealer,
                 Compute => A_AucPrice_Compute),
            Assertions => (Assert_Currency_L1,
                           Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_No_Auction_Price,
                           Assert_Deprecated_AucState)),

         --Connected_Null_Auction_Quote_Dealer,

         TR_Standard_L1_Last_Dealer
           (Stamp      => TR_Monotonize(prices_timestamps_last_dealer,
                                        level => Known_Bug),
            Last_Stamp => prices_timestamps_last_dealer,

            Flag_Map   =>
              ((Normal_Trade               => not Is_Valid(S_TR_TRD_FLG) or S_TR_TRD_FLG <= 1,
                Opening_Auction_Trade      => (S_TR_TRD_FLG <= 3 and B_INST_PHASE <= 2 & 3),
                Closing_Auction_Trade      => (S_TR_TRD_FLG <= 1&3 and B_INST_PHASE <= 6)
                  or (S_TR_TRD_FLG <= 3 and B_INST_PHASE <= 14),
                Intraday_Auction_Trade     => (S_TR_TRD_FLG <= 3 and B_INST_PHASE <= 5),
                --Interruption_Auction_Trade => S_TR_TRD_FLG <= 1,
                --Post_Auction_Trade         => S_TR_TRD_FLG <= 1,
                others                     => null)),

            Compute => not (compute_EOD_trades) and (A_Last_Compute or Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_UNSPECIFIED),

            Ignore  => Meta_Class <= Standard.TR.Refresh
              or Meta_Update <= EMA.RDM.Instrument_Update_Correction
              or (compute_EOD_trades and  Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_UNSPECIFIED),


            Assertions =>
              (Assert_Currency_L1,
               Assert_Currency_Blank_Or_Zero_L1,
               Assert_Currency_Multiplier_L1,
               Assert_SEQNUM_Ordering_L1(Level => Known_Bug))),

         Standard_Aggregated_Last_Dealer
           (Meta         => A_Meta_L1,
            Stamp        => Received,
            When_Release =>
              (Auction_Trades => not (Phase <= Auction_Phase),
               Normal_Trade   => not (Phase <= Trading_Phase),
               others         => null)),

         TR_L2_By_Price_Dealer
           (Summary_Stamp => TR_Monotonize
              (Use_First
                 (Use_First(B_ACTIV_DATE_L2S + S_TIMACT_NS_L2S,Received),
                  Complain(Received,
                           "No summary timestamp supplied",
                           Known_Bug)),
               level => Known_Bug),

            Entry_Stamp   => S_LV_DATE_L2E + S_LV_TIM_MS_L2E,

            Clear_Book    => TR_Non_Empty_Clear_Book                       -- Updates without summary stamp.
              (Contained (Time_Of_Day (Received, ICT_TZ),+"12:06:00", +"12:07:00")),

            Strictness    => (Order_Book_Support.Patient
                              with delta
                              Log_Add    => Known_Bug,
                              Log_Delete => Known_Bug,
                              Log_Update => Known_Bug))));


   Set_Global_Chooser (null);

end Sparta.TR.Exchange_Specifics.Thailand_Stocks;
