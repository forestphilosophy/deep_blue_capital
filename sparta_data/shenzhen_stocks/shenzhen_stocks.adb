with Auction_States;
use  Auction_States;

with New_Lasts;
use  New_Lasts;

with Market_Phases;
use  Market_Phases;

with Market_Codes;
use  Market_Codes;

with Sparta.Choosers;
use  Sparta.Choosers;

with Sparta.Dealers_Manager;
use  Sparta.Dealers_Manager;

with Sparta.Kinds;
use  Sparta.Kinds;

with Sparta.Stubs.Types.Ops;
use  Sparta.Stubs.Types.Ops;

with Sparta.Market_Data.Auctions;

with Sparta.Market_Data.Consumers.Stubs;
use  Sparta.Market_Data.Consumers.Stubs;

with Sparta.Market_Data.Phase_Support;
use  Sparta.Market_Data.Phase_Support;

with Sparta.TR.Metas.Stubs;
use  Sparta.TR.Metas.Stubs;

with Sparta.TR.Fid_Fields.Stubs;
use  Sparta.TR.Fid_Fields.Stubs;

with Time_Zones;
use  Time_Zones;

package body Sparta.TR.Exchange_Specifics.Shenzhen_Stocks is

   use Sparta.Market_Data.Lasts;
   use Sparta.TR.Stub_Types.Enum_T;
   use Sparta.Stubs.Types.Duration_T;

   function TR_China_C1_Auction_State_Dealer
     (Compute : not null access constant Boolean_Stub_Descriptor_Type'Class)
      return Dealers.Dealer_Descriptor_Type_Access
   is (Market_Data.Auctions.Standard_L1_Auction_State_Dealer
            (TimeStamp      => Use_First(S_QUOTE_DATE + Use_First (S_QUOTIM_NS, S_QUOTIM_MS),Received),
             Exchange_Stamp =>  Use_First(S_QUOTE_DATE + Use_First (S_QUOTIM_NS, S_QUOTIM_MS),Received),
        Akind =>
           Auction_State_Kind_Type_T_Logic.If_Then_Else
          (Phase <= Auction_Phase,
           Auction_State_Kind_Type_T_Logic.If_Then_Else
             (Is_Valid (B_IND_AUC) and
                  Is_Valid (B_IND_AUCVOL) and
                  not Equal (B_IND_AUC, 0.0) and
                  not Equal (B_IND_AUCVOL, 0.0),
              Auction_State_Kind_Type_T.Const (Known),
              Auction_State_Kind_Type_T_Logic.If_Then_Else
                ((Is_Valid (B_BEST_BID1) and
                  Is_Valid (B_BEST_BSIZ1) and
                  not Equal (B_BEST_BID1, 0.0) and
                  not Equal (B_BEST_BSIZ1, 0.0)) or
                  (Is_Valid (B_BEST_ASK1) and
                   Is_Valid (B_BEST_ASIZ1) and
                   not Equal (B_BEST_ASK1, 0.0) and
                   not Equal (B_BEST_ASIZ1, 0.0)),
                 Auction_State_Kind_Type_T.Const (Non_Tradable),
                 Auction_State_Kind_Type_T.Const (None))),
           Auction_State_Kind_Type_T.Const (None)),
        Has_Volume => S_True,
        Known_Price   => TR_Construct_Price_L1 (B_IND_AUC),
        Known_Volumes =>
          (Bid => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier *
                  (B_IND_AUCVOL
                   + If_Then_Else
                     (B_IMB_SIDE <= 2,
                      B_IMB_SH, Scalar_T.Const (0.0)))),
           Ask => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier *
                  (B_IND_AUCVOL
                   + If_Then_Else
                     (B_IMB_SIDE <= 3,
                      B_IMB_SH, Scalar_T.Const (0.0))))),
        Guessed_Price_Min => Price_Type_T.Not_Set_Stub,
        Guessed_Price_Max => Price_Type_T.Not_Set_Stub,
        Guessed_Volume    => Unsigned_Volume_Type_T.Not_Set_Stub,
        Non_Tradable_Prices  =>
          (Bid => TR_Construct_Price_L1 (B_BEST_BID1),
           Ask => TR_Construct_Price_L1 (B_BEST_ASK1)),
        Non_Tradable_Volumes =>
          (Bid => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier * B_BEST_BSIZ1),
           Ask => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier * B_BEST_ASIZ1)),
        Meta    => A_Meta_L1,
        Compute => Compute,
        Assertions     =>
          (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
           Assert_Currency_Multiplier_L1,

     Asserts.Boolean_Assert
       (Cond =>
          (  Is_Valid (B_IND_AUC)
             or
             not Is_Valid (B_IND_AUCVOL)
             or not (Date_Type_T_Eqs.Eq                                        --TH_e223341-r696_t8_0x07dc77cac64d79d4_Full.csv.gz 2022-01-12T01:15:09.711842598Z Auction volume with no Auction price
                    (Date_Of (Received), Dates.Date_Of ((2022, 1, 21)))
             and Contained
                       (Time_Of_Day (Received, CST_TZ),+"09:15:00", +"09:16:00"))),

         Msg => "Auction volume with no Auction price",
          Log => Except),


           Asserts.Boolean_Assert
             (A_L2_Only
              or Is_Valid (B_BEST_BID1)
              or not Is_Valid (B_BEST_BSIZ1)
              or not (Date_Type_T_Eqs.Eq                                        --TH_e216941-r668_t8_0x07d9331ad06d7310_Full.csv.gz 2020-07-21T01:31:09.656431406Z only stamp during the entire period where the BEST_BID1 was not distributed before
                      (Date_Of (Received), Dates.Date_Of ((2020, 7, 21)))
                      and Contained
                        (Time_Of_Day (Received, CST_TZ),+"01:30:00", +"01:32:00")),
              "Auction BID volume with no Auction BID price",
              Kinds.Except),
           Asserts.Boolean_Assert
             (A_L2_Only
              or Is_Valid (B_BEST_ASK1)
              or not Is_Valid (B_BEST_ASIZ1)
              or not (Date_Type_T_Eqs.Eq                                        --TH_e216997-r668_t8_0x07d944f3a9ad732a_Full.csv.gz 2020-07-21T01:31:10.136284014Z same as above but with BEST_ASK1
                      (Date_Of (Received), Dates.Date_Of ((2020, 7, 21)))
                       and Contained
                        (Time_Of_Day (Received, CST_TZ),+"01:30:00", +"01:32:00")),
              "Auction ASK volume with no Auction ASK price",
              Kinds.Except)
          )));

begin
   Set_Global_Chooser
     (Data_Vendor (TR.Reuters_Data_Vendor));
   -- !!!!!!!!     Add "Register" after this line.     !!!!!!!!!

   Register
     (Mkt (MKT_XSHE), Tag => "Shenzhen_Stocks",
      Descriptors =>
        (TR_Standard_L1_Price_Dealer_B
             (Stamp   =>
                  Use_First(B_QUOTE_DATE + Use_First(S_QUOTIM_NS, S_QUOTIM_MS), Received),

              Size => (Bid => Unsigned_Volume_Type_T_Logic.If_Then_Else         -- TH_e223449-r696_t8_0x07dc6c9e740d79cc_Full.csv.gz 2020-07-21T01:31:10.136325743Z sparta complains about no bid/ask prices while receiving bid/ask sizes, these orders are non market orders sent during the trading phase that happened only on 2020-07-21 => creating blank stubs
                       		(B_INST_PHASE <= 7 and Is_Blank(B_BID) and not Is_Blank(B_BIDSIZE),
                            	Unsigned_Volume_Type_T.Blank_Stub,
                                AB_BIDSIZE_M),

                        ASK => Unsigned_Volume_Type_T_Logic.If_Then_Else
                       		(B_INST_PHASE <= 7 and Is_Blank(B_ASK) and not Is_Blank(B_ASKSIZE),
                            	Unsigned_Volume_Type_T.Blank_Stub,
                          AB_Asksize_M)),

              Price => (Bid => Price_Type_T_Logic.If_Then_Else                  -- TH_e223361-r696_t8_0x07dc77c6061d79d4_Full.csv.gz 2020-07-21T01:31:09.76437637Z same as above but valid bid/ask while not receiving bid/ask sizes
                       		(B_INST_PHASE <= 7 and Is_Valid(B_BID) and not Is_Valid(B_BIDSIZE),
                            	Price_Type_T.Blank_Stub,
                                AB_BID),

                        ASK => Price_Type_T_Logic.If_Then_Else
                       		(B_INST_PHASE <= 7 and Is_Valid(B_ASK) and not Is_Valid(B_ASKSIZE),
                            	Price_Type_T.Blank_Stub,
                                AB_ASK)),

              Compute => A_Price_Compute_Unspecified and                        -- Bid and asks are sent during the aftermarket phase
                         not(B_INST_PHASE <= 1 & 11),                           -- with no sizes (do not compute bids on that phase)

              Assertions => (Assert_Currency_L1,
                             Assert_Currency_Blank_Or_Zero_L1,
                             Assert_Currency_Multiplier_L1,
                             Assert_SEQNUM_Ordering_L1)
              & Assert_Price_Not_Valid_When_Not_Compute
                (Compute_Label => "TR_QUOTE_B_COMPUTE",
                 Ignore        => Meta_Class <= Standard.TR.Refresh
                 		or Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_CORRECTION
                 		or B_INST_PHASE<= 11)),

         Tr_Standard_L1_Opening_Price_Dealer
           (Stamp => Monotonize (Use_First(
            Use_First(
              Use_First(B_TRADE_DATE + S_SALTIM_MS, B_Quote_Date + S_QUOTIM_MS),
              Combine(Received, S_EXCHTIM)),
            Received))),

         Tr_Standard_L1_Closing_Price_Dealer (Received),

         TR_L2_By_Level_Dealer
           (Summary_Stamp => Received,

            Entry_Stamp   => B_ACTIV_DATE_L2S + Use_First (S_LV_TIM_NS_L2E, S_LV_TIM_MS_L2E),

            Compute_Update => not(B_INST_PHASE_L2S <= 7 and                     -- TH_e223362-r696_t9_0x07dc49462a7d7993_Full.csv.gz 2020-07-21T01:31:09.764381842Z no bid price while receiving valid bid size, happens only on 2020-07-01 => not computing
                  		not (Is_Valid(S_ACC_SIZE) and Is_Valid(S_ORDER_PRC))),

            Strictness    => (Order_Book_Support.Patient
                               with delta
                                 Log_Add => Known_Bug,
                               Log_Delete  => Known_Bug,
                               Log_Update  => Known_Bug)),

         TR_Standard_L1_Phase_Dealer
             (Stamp => Use_First(Combine(Received, Use_First(S_QUOTIM_MS, S_INDAUC_MS)), Received),
              Compute => A_Phase_Compute,
              Map => (Auction_Phase        => B_INST_PHASE <= 2 & 4 & 6,
                      No_Trading_Phase     => B_INST_PHASE <= 0 & 1 & 7 & 9 & 11,
                      Trading_Phase        => B_INST_PHASE <= 3,
                      Unknown_Phase        => not Is_Valid(B_INST_PHASE),
                      others               => null)),

         TR_China_C1_Auction_State_Dealer
           (Compute =>
                A_L1_Only and
              (Meta_Class <= Standard.TR.Refresh
               or
                 Meta_Update <= (EMA.RDM.INSTRUMENT_UPDATE_UNSPECIFIED,
                                 EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN,
                                 EMA.RDM.INSTRUMENT_UPDATE_MARKET_DIGEST))),

         TR_Standard_L1_Last_Dealer
           (Stamp      => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),

            Last_Stamp => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),

            Compute    => A_Last_Compute or
              Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_UNSPECIFIED,

            Assertions => (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_SEQNUM_Ordering_L1 (Known_Bug)
                          ),

            Flag_Map =>
              (Normal_Trade               => B_INST_PHASE <= 3 & 6 or
                                            (B_INST_PHASE <= 9 and
                                             Contained (Time_Of_Day (Received, CST_TZ), +"11:30:00", +"11:30:05")),

               Opening_Auction_Trade      => B_INST_PHASE <= 2 & 4 or
                                            (B_INST_PHASE <= 9 and
                                             Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:25:05")),

               Closing_Auction_Trade      => B_INST_PHASE <= 0 & 1,
               None                       => B_INST_PHASE <= 7 or
                                            (B_INST_PHASE <= 9 and not
                                            (Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:25:05") or
                                             Contained (Time_Of_Day (Received, CST_TZ), +"11:30:00", +"11:30:05"))),

               others                     => null)),

         Standard_Aggregated_Last_Dealer
           (Stamp => Received,
            Meta  => A_Meta_L1,
            When_Release =>
              (Auction_Trades => not (Phase <= Auction_Phase),
               Normal_Trade   => not (Phase <= Trading_Phase),
               others => null))));


   -- !!!!!!!!     Add "Register" before this line.     !!!!!!!!!
   Set_Global_Chooser (null);
end Sparta.TR.Exchange_Specifics.Shenzhen_Stocks;
