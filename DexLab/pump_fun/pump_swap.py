from solders.transaction import VersionedTransaction # type: ignore
from solders.keypair import Keypair # type: ignore
from solders.pubkey import Pubkey as PublicKey # type: ignore
from solders import message
from solana.rpc.async_api import AsyncClient
from solana.rpc.types import TxOpts
from solders.transaction import Transaction # type: ignore
from solders.instruction import AccountMeta, Instruction # type: ignore
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import (
    create_idempotent_associated_token_account,
    get_associated_token_address,
)
import base58
from borsh_construct import CStruct, U64
import logging
import asyncio, json
from solders.compute_budget import set_compute_unit_price # type: ignore
from solders.system_program import transfer, TransferParams # type: ignore
from aiohttp import ClientSession
import time, requests
import struct
from typing import List, Optional
from decimal import Decimal
try: from .pump_bond import get_bonding_curve_state
except: from .pump_bond import get_bonding_curve_state

from solana.rpc.commitment import Processed
from solders.message    import MessageV0 # type: ignore

PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
GLOBAL_VOLUME_ACCUMULATOR = "Hq2wp8uJ9jCPsYgNHex8RtqdvMPfVGoYwjvF1ATiwn2Y"
FEE_CONFIG = "8Wf5TiAheLUqBrKXeYg2JtAFFMWtKdG2BSFgqUcPVwTt"
FEE_PROGRAM = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ"
DEFAULT_FEE_RECIPIENT = "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV"
DEFAULT_BUY_QUOTE_FEE_BPS = 125
MAYHEM_FEE_RECIPIENTS = [
    "GesfTA3X2arioaHp8bbKdjG9vJtskViWACZoYvxp4twS",
    "4budycTjhs9fD6xw62VBducVTNgMgJJ5BgtKq7mAZwn6",
    "8SBKzEQU4nLSzcwF4a74F2iaUDQyTfjGndn6qUWBnrpR",
    "4UQeTP1T39KZ9Sfxzo3WR5skgsaP6NZa87BAkuazLEKH",
    "8sNeir4QsLsJdYpc9RZacohhK1Y5FLU3nC5LXgYB4aa6",
    "Fh9HmeLNUMVCvejxCtCL2DbYaRyBFVJ5xrWkLnMH6fdk",
    "463MEnMeGyJekNZFQSTUABBEbLnvMTALbT6ZmsxAbAdq",
    "6AUH3WEHucYZyC61hqpqYUWVto5qA5hjHuNQ32GNnNxA",
]
TOKEN_2022_PROGRAM_ID = PublicKey.from_string("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
LEGACY_TOKEN_PROGRAM_ID = PublicKey.from_string(str(TOKEN_PROGRAM_ID))

BUY_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "max_sol_cost" / U64
)

BUY_EXACT_SOL_IN_DISCRIMINATOR = bytes([56, 252, 116, 8, 158, 223, 205, 95])
SELL_INSTRUCTION_SCHEMA = CStruct(
    "amount" / U64,
    "min_sol_output" / U64
)

BUY_DISCRIMINATOR = bytes([102, 6, 61, 18, 1, 218, 235, 234])
SELL_DISCRIMINATOR = bytes([51, 230, 133, 164, 1, 127, 131, 173])

suppress_logs = [
    "socks",
    "requests",
    "httpx",
    "trio.async_generator_errors",
    "trio",
    "trio.abc.Instrument",
    "trio.abc",
    "trio.serve_listeners",
    "httpcore.http11",
    "httpcore",
    "httpcore.connection",
    "httpcore.proxy",
]

# Set all of them to CRITICAL (no logs)
for log_name in suppress_logs:
    logging.getLogger(log_name).setLevel(logging.CRITICAL)
    logging.getLogger(log_name).handlers.clear()
    logging.getLogger(log_name).propagate = False

def get_solana_price_usd():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd')
        data = response.json()
        price = data['solana']['usd']
        logging.info(f"Solana price: {price}")
        return str(price)
    except Exception:
        logging.info(f"Failed to get Solana price from Coingecko")
        time.sleep(5)
        return get_solana_price_usd()

class PumpFun:
    def __init__(self, session: ClientSession, priv_key: str, async_client: AsyncClient):
        self.session = session
        self.priv_key = Keypair.from_bytes(
                base58.b58decode(str(priv_key))
            )
        self.async_client = async_client

    def _derive_uva_pda(self, payer: PublicKey):
        user_acc, _ = PublicKey.find_program_address(
            [b"user_volume_accumulator", bytes(payer)], PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
        )
        return user_acc

    async def get_token_program_id(self, token_address: PublicKey) -> PublicKey:
        """
        Fetch the owner of the mint account to determine which token program it uses.
        """
        info = await self.async_client.get_account_info(token_address, commitment=Processed)
        account_info = getattr(info, "value", None)
        if account_info is None:
            raise Exception("account not found")

        owner = getattr(account_info, "owner", None)
        if owner is None and isinstance(account_info, dict):
            owner = account_info.get("owner")
        if owner is None:
            raise Exception("owner not found for token account")

        return PublicKey.from_string(str(owner))

    async def get_ata_auto(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        token_program_id = await self.get_token_program_id(mint)
        if token_program_id == TOKEN_2022_PROGRAM_ID:
            return self.get_ata_for_token2022(owner, mint)
        if token_program_id == LEGACY_TOKEN_PROGRAM_ID:
            return self.get_ata_for_token(owner, mint)
        raise Exception("Invalid token program id")

    def get_ata_for_token(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        return get_associated_token_address(owner, mint, LEGACY_TOKEN_PROGRAM_ID)

    def get_ata_for_token2022(self, owner: PublicKey, mint: PublicKey) -> PublicKey:
        return get_associated_token_address(owner, mint, TOKEN_2022_PROGRAM_ID)

    def get_bonding_curve_v2_pda(self, mint: PublicKey) -> PublicKey:
        bonding_curve_v2, _ = PublicKey.find_program_address(
            [b"bonding-curve-v2", bytes(mint)],
            PublicKey.from_string(PUMP_FUN)
        )
        return bonding_curve_v2

    def get_fee_recipient(self, curve_state) -> PublicKey:
        if curve_state is not None and getattr(curve_state, "is_mayhem_mode", False):
            return PublicKey.from_string(MAYHEM_FEE_RECIPIENTS[0])
        return PublicKey.from_string(DEFAULT_FEE_RECIPIENT)

    def quote_buy_exact_sol_in_tokens_out(self, curve_state, spendable_sol_in: int) -> int:
        if curve_state is None:
            return 0

        virtual_sol_reserves = int(getattr(curve_state, "virtual_sol_reserves", 0))
        virtual_token_reserves = int(getattr(curve_state, "virtual_token_reserves", 0))
        if spendable_sol_in <= 0 or virtual_sol_reserves <= 0 or virtual_token_reserves <= 0:
            return 0

        total_fee_bps = DEFAULT_BUY_QUOTE_FEE_BPS
        net_sol = (spendable_sol_in * 10_000) // (10_000 + total_fee_bps)
        fees = (net_sol * total_fee_bps + 9_999) // 10_000
        if net_sol + fees > spendable_sol_in:
            net_sol -= (net_sol + fees - spendable_sol_in)

        if net_sol <= 1:
            return 0

        return ((net_sol - 1) * virtual_token_reserves) // (virtual_sol_reserves + net_sol - 1)

    def quote_curve_price(self, curve_state) -> Decimal:
        if curve_state is None:
            return Decimal("0")

        virtual_sol_reserves = Decimal(int(getattr(curve_state, "virtual_sol_reserves", 0))) / Decimal("1e9")
        virtual_token_reserves = Decimal(int(getattr(curve_state, "virtual_token_reserves", 0))) / Decimal("1e6")
        if virtual_sol_reserves <= 0 or virtual_token_reserves <= 0:
            return Decimal("0")
        return virtual_sol_reserves / virtual_token_reserves

    def quote_sell_exact_tokens_in_sol_out(self, curve_state, token_amount: int) -> int:
        if curve_state is None:
            return 0

        virtual_sol_reserves = int(getattr(curve_state, "virtual_sol_reserves", 0))
        virtual_token_reserves = int(getattr(curve_state, "virtual_token_reserves", 0))
        if token_amount <= 0 or virtual_sol_reserves <= 0 or virtual_token_reserves <= 0:
            return 0

        gross_sol_out = (token_amount * virtual_sol_reserves) // (virtual_token_reserves + token_amount)
        if gross_sol_out <= 0:
            return 0

        return max((gross_sol_out * (10_000 - DEFAULT_BUY_QUOTE_FEE_BPS)) // 10_000, 0)

    async def paper_buy_quote(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: Optional[str] = None,
        ):
        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, _ = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping paper buy for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        token_amount = self.quote_buy_exact_sol_in_tokens_out(curve_state, sol_amount)
        if token_amount <= 0:
            return "zero_quote"

        return {
            "fill_qty": token_amount,
            "fill_price": self.quote_curve_price(curve_state),
            "lamports_spent": int(sol_amount),
        }

    async def paper_sell_quote(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            token_amount: int,
            creator: Optional[str] = None,
        ):
        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, _ = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping paper sell for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        lamports_out = self.quote_sell_exact_tokens_in_sol_out(curve_state, token_amount)
        if lamports_out <= 0:
            return "zero_quote"

        return {
            "fill_price": self.quote_curve_price(curve_state),
            "lamports_out": int(lamports_out),
        }

    async def get_curve_context(
        self,
        bonding_curve: PublicKey,
        fallback_creator: Optional[str] = None,
        allow_creator_fallback: bool = False,
        retries: int = 8,
        retry_delay_seconds: float = 0.15,
    ):
        curve_state = None

        for attempt in range(retries):
            curve_state = await get_bonding_curve_state(self.async_client, bonding_curve)
            if curve_state is not None:
                if getattr(curve_state, "complete", False):
                    return curve_state, None

                creator = getattr(curve_state, "creator", None)
                if creator and any(creator):
                    creator_vault = self.get_creator_vault(
                        base58.b58encode(creator).decode("utf-8")
                    )
                    return curve_state, creator_vault

            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_seconds)

        if allow_creator_fallback and fallback_creator:
            logging.warning(
                "Falling back to the caller-provided creator after retries failed to resolve the on-chain creator."
            )
            return curve_state, self.get_creator_vault(fallback_creator)

        raise ValueError("Unable to resolve creator_vault from the bonding curve state after retries.")

    def build_buy_accounts(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> List[AccountMeta]:
        buyer = self.priv_key.pubkey()
        return [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False), # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),         # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True), # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedBondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(buyer, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                    # associatedUser
            AccountMeta(pubkey=buyer, is_signer=True, is_writable=True),         # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=token_program_id, is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=creator_vault, is_signer=False, is_writable=True), # creatorVault
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False), # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),   # program
            AccountMeta(pubkey=PublicKey.from_string(GLOBAL_VOLUME_ACCUMULATOR), is_signer=False, is_writable=True), # globalVolumeAccumulator
            AccountMeta(pubkey=self._derive_uva_pda(buyer), is_signer=False, is_writable=True), # userVolumeAccumulator
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]

    async def build_buy_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,      # how many tokens to buy
        lamports_budget: int,    # how many lamports to spend
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> Instruction:
        instruction_data = (
            BUY_DISCRIMINATOR
            + BUY_INSTRUCTION_SCHEMA.build({
                "amount": token_amount,
                "max_sol_cost": lamports_budget
            })
            + b"\x01\x01"
        )
        accounts = self.build_buy_accounts(
            mint=mint,
            bonding_curve=bonding_curve,
            fee_recipient=fee_recipient,
            creator_vault=creator_vault,
            token_program_id=token_program_id,
        )
        accounts.append(
            AccountMeta(
                pubkey=self.get_bonding_curve_v2_pda(mint),
                is_signer=False,
                is_writable=False
            )
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def build_buy_exact_sol_in_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        spendable_sol_in: int,
        min_tokens_out: int,
        creator_vault: PublicKey,
        token_program_id: PublicKey
    ) -> Instruction:
        # OptionBool::None is encoded as a single 0 byte.
        instruction_data = (
            BUY_EXACT_SOL_IN_DISCRIMINATOR
            + struct.pack("<QQ", spendable_sol_in, min_tokens_out)
            + b"\x00"
        )

        accounts = self.build_buy_accounts(
            mint=mint,
            bonding_curve=bonding_curve,
            fee_recipient=fee_recipient,
            creator_vault=creator_vault,
            token_program_id=token_program_id,
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def build_sell_instruction(
        self,
        mint: PublicKey,
        bonding_curve: PublicKey,
        fee_recipient: PublicKey,
        token_amount: int,       # how many tokens to sell
        lamports_min_output: int, # minimum lamports you want to receive
        vault: PublicKey,
        token_program_id: PublicKey,
        cashback: bool = False
    ) -> Instruction:
        instruction_data = SELL_DISCRIMINATOR + SELL_INSTRUCTION_SCHEMA.build({
            "amount": token_amount,
            "min_sol_output": lamports_min_output
        })

        user = self.priv_key.pubkey()

        # The IDL's account list for sell:
        accounts = [
            AccountMeta(pubkey=PublicKey.from_string("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf"), is_signer=False, is_writable=False),  # global
            AccountMeta(pubkey=fee_recipient, is_signer=False, is_writable=True),  # feeRecipient
            AccountMeta(pubkey=mint, is_signer=False, is_writable=False),          # mint
            AccountMeta(pubkey=bonding_curve, is_signer=False, is_writable=True),  # bondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(bonding_curve, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedBondingCurve
            AccountMeta(
                pubkey=get_associated_token_address(user, mint, token_program_id),
                is_signer=False,
                is_writable=True
            ),                                                                     # associatedUser
            AccountMeta(pubkey=user, is_signer=True, is_writable=True),           # user
            AccountMeta(pubkey=PublicKey.from_string("11111111111111111111111111111111"), is_signer=False, is_writable=False), # systemProgram
            AccountMeta(pubkey=PublicKey.from_string(str(vault)), is_signer=False, is_writable=True),  # vault
            AccountMeta(pubkey=token_program_id, is_signer=False, is_writable=False), # tokenProgram
            AccountMeta(pubkey=PublicKey.from_string("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1"), is_signer=False, is_writable=False),  # eventAuthority
            AccountMeta(pubkey=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"), is_signer=False, is_writable=False),    # program
            AccountMeta(pubkey=PublicKey.from_string(FEE_CONFIG), is_signer=False, is_writable=False), # feeConfig
            AccountMeta(pubkey=PublicKey.from_string(FEE_PROGRAM), is_signer=False, is_writable=False), # feeProgram
        ]
        if cashback:
            accounts.append(
                AccountMeta(pubkey=self._derive_uva_pda(user), is_signer=False, is_writable=True)
            )
        accounts.append(
            AccountMeta(pubkey=self.get_bonding_curve_v2_pda(mint), is_signer=False, is_writable=False)
        )

        return Instruction(
            program_id=PublicKey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
            accounts=accounts,
            data=instruction_data
        )

    async def check_ata_exists(self, owner: PublicKey, mint: PublicKey, token_program_id: PublicKey) -> bool:
        """
        Check if the associated token account (ATA) exists on-chain.
        """
        ata_address = get_associated_token_address(owner, mint, token_program_id)

        try:
            response = await self.async_client.get_account_info(ata_address)
            if response.value:
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Error checking ATA existence: {e}")
            return False

    async def make_check_ata(self, instructions: list, mint_address: PublicKey, token_program_id: PublicKey):
        """
        Check if the Associated Token Account (ATA) exists.
        If it doesn't, add an instruction to create it for the provided token program.
        """
        owner = self.priv_key.pubkey()
        
        # Check if the ATA exists
        ata_exists = await self.check_ata_exists(owner, mint_address, token_program_id)
        
        if not ata_exists:
            instructions.append(
                create_idempotent_associated_token_account(
                    payer=owner,
                    owner=owner,
                    mint=mint_address,
                    token_program_id=token_program_id
                )
            )

        return instructions

    def get_creator_vault(self, creator):
        creator_vault_pda, _ = PublicKey.find_program_address(
            [b"creator-vault", bytes(PublicKey.from_string(creator))],
            PublicKey.from_string(PUMP_FUN)
        )
        return creator_vault_pda

    async def _compile_transaction(self, instructions: list[Instruction | Transaction]):
        try:
            latest_blockhash = (await self.async_client.get_latest_blockhash(commitment=Processed)).value.blockhash
            msg = MessageV0.try_compile(
                payer=self.priv_key.pubkey(),
                instructions=instructions,
                address_lookup_table_accounts=[],
                recent_blockhash=latest_blockhash
            )
            return VersionedTransaction(msg, [self.priv_key])
        except Exception as e:
            logging.error(f"Failed to fetch latest blockhash: {e}")
            raise

    async def _simulate_or_send(self, tx: VersionedTransaction, sim: bool = False):
        try:
            if sim:
                return await self.async_client.simulate_transaction(
                    tx,
                    sig_verify=False,
                    commitment=Processed
                )

            opts = TxOpts(skip_preflight=True, skip_confirmation=True)
            result = await self.async_client.send_transaction(tx, opts=opts)
            result_json = result.to_json()
            return json.loads(result_json).get('result')
        except Exception as e:
            logging.error(f"Transaction failed: {e}")
            raise

    async def pump_buy(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: str,
            token_amount: int = 0,
            sim: bool = False,
            priority_micro_lamports: int = 0,
            slippage: float = 1.3, # MAX: 1.99
            skip_ata_check: bool = False,
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping buy for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        if token_amount <= 0:
            token_amount = self.quote_buy_exact_sol_in_tokens_out(curve_state, sol_amount)
            if token_amount <= 0:
                return "zero_quote"

        token_program_id = await self.get_token_program_id(mint_address)

        if priority_micro_lamports > 0:
            instructions.append(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )

        if not skip_ata_check:
            instructions = await self.make_check_ata(instructions, mint_address, token_program_id)

        fee_recipient = self.get_fee_recipient(curve_state)
        buy_ix = await self.build_buy_instruction(
            mint_address,
            bonding_curve_pda,
            fee_recipient,
            token_amount,
            # slippage, 1.99x
            int(sol_amount * slippage),
            creator_vault,
            token_program_id
        )
        instructions.append(buy_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim)

    async def pump_buy_exact_sol_in(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            sol_amount: int,
            creator: Optional[str] = None,
            sim: bool = False,
            priority_micro_lamports: int = 0,
            slippage: float = 1.3,
            skip_ata_check: bool = False,
        ):

        if sol_amount <= 0:
            return "zero_sol_amount"

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping buy_exact_sol_in for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        quote_tokens_out = self.quote_buy_exact_sol_in_tokens_out(curve_state, sol_amount)
        min_tokens_out = int(quote_tokens_out / slippage) if slippage > 0 else 0
        if min_tokens_out <= 0:
            return "zero_quote"

        token_program_id = await self.get_token_program_id(mint_address)

        if priority_micro_lamports > 0:
            instructions.append(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )

        if not skip_ata_check:
            instructions = await self.make_check_ata(instructions, mint_address, token_program_id)

        fee_recipient = self.get_fee_recipient(curve_state)
        buy_ix = await self.build_buy_exact_sol_in_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            spendable_sol_in=sol_amount,
            min_tokens_out=min_tokens_out,
            creator_vault=creator_vault,
            token_program_id=token_program_id
        )
        instructions.append(buy_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim)

    async def pump_sell(
            self,
            mint_address: str,
            bonding_curve_pda: str,
            token_amount: int,
            lamports_min_output: int,
            creator: str,
            sim: bool = False,
            priority_micro_lamports: int = 0
        ):

        instructions = []

        mint_address = PublicKey.from_string(mint_address)
        bonding_curve_pda = PublicKey.from_string(bonding_curve_pda)

        try:
            curve_state, creator_vault = await self.get_curve_context(bonding_curve_pda)
        except ValueError as exc:
            logging.warning(f"Skipping sell for {mint_address}: {exc}")
            return "creator_vault_unavailable"
        if curve_state is not None and getattr(curve_state, "complete", False):
            return "migrated"

        token_program_id = await self.get_token_program_id(mint_address)
        if not await self.check_ata_exists(self.priv_key.pubkey(), mint_address, token_program_id):
            return "missing_ata"
        
        if priority_micro_lamports > 0:
            instructions.append(
                set_compute_unit_price(
                    priority_micro_lamports
                )
            )

        fee_recipient = self.get_fee_recipient(curve_state)
        sell_ix = await self.build_sell_instruction(
            mint=mint_address,
            bonding_curve=bonding_curve_pda,
            fee_recipient=fee_recipient,
            token_amount=token_amount,
            lamports_min_output=lamports_min_output,
            vault=creator_vault,
            token_program_id=token_program_id,
            cashback=bool(getattr(curve_state, "is_cashback_coin", False))
        )
        instructions.append(sell_ix)

        tx = await self._compile_transaction(instructions)
        return await self._simulate_or_send(tx, sim=sim)

    async def getTransaction(self, tx_id: str, session: ClientSession):
        start_time = time.time()
        attempt = 1
        try:
            while attempt < 25:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        tx_id,
                        {
                            "commitment": "confirmed",
                            "encoding": "json",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }
                headers = {
                    "Content-Type": "application/json"
                }

                async with session.post(self.rpc_endpoint, json=payload, headers=headers, timeout=10) as response:
                    if response.status != 200:
                        logging.error(f"HTTP Error {response.status}: {await response.text()}")
                        raise Exception(f"HTTP Error {response.status}")

                    data = await response.json()
                    logging.info(f"Attempt {attempt}")

                    if data and data.get('result') is not None:
                        logging.info(f"Elapsed: {time.time() - start_time:.2f}s")
                        result = data['result']
                        return result

                await asyncio.sleep(0.5)
                attempt += 1
        except Exception as e:
            logging.error(f"Error: {e}")
            return None

    async def close(self):
        await self.async_client.close()
        await self.session.close()
