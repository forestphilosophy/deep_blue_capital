with New_Lasts;
use  New_Lasts;

with Market_Phases;
use  Market_Phases;

with Market_Codes;
use  Market_Codes;

with Symbol_Kinds;
use  Symbol_Kinds;

with Sparta.Choosers;
use  Sparta.Choosers;

with Sparta.Dealers_Manager;
use  Sparta.Dealers_Manager;

with Sparta.Kinds;
use  Sparta.Kinds;

with Sparta.Stubs.Types.Ops;
use  Sparta.Stubs.Types.Ops;

with Sparta.Market_Data.Consumers.Stubs;
use  Sparta.Market_Data.Consumers.Stubs;

with Sparta.Market_Data.Phase_Support;
use  Sparta.Market_Data.Phase_Support;

with Sparta.TR.Metas.Stubs;
use  Sparta.TR.Metas.Stubs;

with Sparta.TR.Fid_Fields.Stubs;
use  Sparta.TR.Fid_Fields.Stubs;

package body Sparta.TR.Exchange_Specifics.Osaka_Futures is

   use Sparta.TR.Stub_Types.Enum_T;
   use Sparta.Market_Data.Lasts;
   use Sparta.Stubs.Types.Duration_T;

   Timestamp_Prices: constant access constant Ada_Time_T.Target_Stub_Descriptor_Type'Class := B_QUOTE_DATE + Use_First(S_QUOTIM_MS,S_QUOTIM);


begin
   Set_Global_Chooser
     (Data_Vendor (TR.Reuters_Data_Vendor));
   -- !!!!!!!!     Add "Register" after this line.     !!!!!!!!!

   Register
     (Mkt (MKT_XJPX) and Symbol_Kind(Symbol_Kinds.Kind_Future),
      Tag => "OSAKA_Futures",
      Descriptors =>
        (TR_Standard_L1_Phase_Dealer
           (Stamp => Received,
            Compute => A_Phase_Compute,
            Map =>
              (Unknown_Phase        => not Is_Valid(B_INST_PHASE),
               No_Trading_Phase     => B_INST_PHASE <= 1 or (B_INST_PHASE <= 15 and B_PERIOD_CD2 <= "CLOSE"), -- we receive intraday auction phases during non trading hours => using PERIOD_CD2 to define non trading phase
               Trading_Phase        => B_INST_PHASE <= 3,
               Auction_Phase        => B_INST_PHASE <= 2 & 6 or (B_INST_PHASE <= 15 and not (B_PERIOD_CD2 <= "CLOSE")),
               others               => null)),

         TR_Standard_L1_Price_Dealer_B_Nondeprecated
           (Stamp   => If_Then_Else
              (Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN
                 or
                 Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_QUOTE
                 or Meta_Class <= Standard.TR.Update
                 or Meta_Class <= Standard.TR.Refresh,
               Use_First (Timestamp_Prices, Received),
               Timestamp_Prices),

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


            Compute => A_Compute_Nondeprecated_B
              (Target  => Timestamp_Prices,
               Compute => A_Price_Compute)),


         TR_Standard_L1_Last_Dealer_Noduplicates
           (Stamp      => TR_Monotonize
              (Use_First(B_TRADE_DATE + S_SALTIM_MS,
                         Combine(Received, S_SALTIM_MS))),

            Last_Stamp => TR_Monotonize
              (Use_First(B_TRADE_DATE + S_SALTIM_MS,
                         Combine(Received, S_SALTIM_MS))),

            Flag_Map  =>
              ((Normal_Trade               => B_INST_PHASE <= 3 or (B_INST_PHASE <= 2 and not (S_PRC_QL_CD <= 0)),
                Opening_Auction_Trade      => B_INST_PHASE <= 2 and S_PRC_QL_CD <= 0,
                Closing_Auction_Trade      => B_INST_PHASE <= 1 & 6,
                Intraday_Auction_Trade     => B_INST_PHASE <= 15,
                others                     => null)),

            Compute => A_Last_Compute or
              (Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_UNSPECIFIED),

            Assertions => (Assert_Currency_L1,
                           Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_Duplicated_Trade_Seqnum)),

         Auctions.Standard_L1_Auction_State_Dealer
           (Meta           => A_Meta_L1,
            TimeStamp      => S_QUOTE_DATE + Use_First (S_QUOTIM_NS, S_QUOTIM_MS),
            Exchange_Stamp => S_QUOTE_DATE + Use_First (S_QUOTIM_NS, S_QUOTIM_MS),

            Akind =>
              Auction_State_Kind_Type_T_Logic.If_Then_Else
                (not (Phase <= Auction_Phase)
                   or (Phase <= Auction_Phase and
                         Upper_Value_Stamp (S_INST_PHASE) + 30.0 <= Upper_Value_Stamp (S_TRDPRC_1)),
                 --switch off auction phase once the first auction trade occurs to prevent bogus non tradable price auction states
                 Auction_State_Kind_Type_T.Const (None),
                 Auction_State_Kind_Type_T_Logic.If_Then_Else
                   (Eq (B_BID, B_ASK),
                    Auction_State_Kind_Type_T.Const (Known),
                    Auction_State_Kind_Type_T.Const (Non_Tradable))),

            Has_Volume => Boolean_T.Const (True),

            Known_Price   => Tr_Construct_Price_L1 (B_BID),
            Known_Volumes =>
              (Bid => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_BIDSIZE),
               Ask => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_ASKSIZE)),

            Guessed_Price_Min => Price_Type_T.Not_Set_Stub,
            Guessed_Price_Max => Price_Type_T.Not_Set_Stub,
            Guessed_Volume    => Unsigned_Volume_Type_T.Not_Set_Stub,

            Non_Tradable_Prices   =>
              (Bid => Tr_Construct_Price_L1 (B_BID),
               Ask => Tr_Construct_Price_L1 (B_ASK)),
            Non_Tradable_Volumes =>
              (Bid => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_BIDSIZE),
               Ask => Construct_Unsigned_Volume (Info_Volume_Lot_Multiplier * B_ASKSIZE)),

            Compute => A_L1_Only and A_Aucprice_Compute
              and (Is_Valid (B_BID) and Is_Valid (B_ASK))                        -- 2021-02-28T23:00:10.629621417Z TH_JNMK1_t8_0x07c09fca7a3d3c18_Full.csv.gz sparta complains about bad non tradeble prices at side ask because there was no valid ask price before to construct non tradable price
              and (Is_Valid (S_BID) or Is_Valid (S_ASK))),



         Standard_Aggregated_Last_Dealer
           (Stamp => Received,
            Meta  => A_Meta_L1,
            When_Release =>
              (Auction_Trades => S_True, -- There can be only one Auction trade
               Normal_Trade   =>
                 not (Phase <= Trading_Phase)
                   and Received <= -- Some Normal trades are distributed at the end of Auction phase.
                   Save_At (Changed (B_INST_PHASE), Received) + 60.0,
               others => null)),

         TR_Standard_L1_Opening_Price_Dealer
           (Stamp => Use_First(
                               Use_First(B_TRADE_DATE + S_SALTIM_MS,
                                         Combine(Received, S_SALTIM_MS)),
                               Received)),

         TR_Standard_L1_Closing_Price_Dealer
           (Stamp => Use_First(B_OFF_CLS_DT + S_SETTLE_TIM, Received)),

         TR_L2_By_Price_Dealer
           (Summary_Stamp => TR_Monotonize
              (Use_First(TR_Stamp_Unspecified_With_Received
                           (B_ACTIV_DATE_L2S+S_TIMACT_MS_L2S,
                            B_ACTIV_DATE_L2S+B_TIMACT_MS_L2S,
                            B_ACTIV_DATE_L2S+S_TIMACT_MS_L2S),
                         Complain(Received,
                                  "No summary timestamp supplied",
                                  Known_Bug)),
               Level=> Known_Bug),

            Entry_Stamp => S_LV_DATE_L2E + S_LV_TIM_MS_L2E,

            Strictness => (Order_Book_Support.Patient with delta
                           Log_Add => Known_Bug,
                           Log_Delete  => Known_Bug,
                           Log_Update  => Known_Bug))));

   -- !!!!!!!!     Add "Register" before this line.     !!!!!!!!!
   Set_Global_Chooser (null);
end Sparta.TR.Exchange_Specifics.Osaka_Futures;
